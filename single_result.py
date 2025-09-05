# file: spark_queries_single_results.py
# pyspark==3.x
from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.types import *
import datetime

# ====== 读取 Postgres 连接参数 ======
from utils.constants import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
RESULTS_TABLE = "case_results"   # 单一结果表

def read_table(spark, table):
    return (spark.read
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .options(**PG_PROPS)
        .load())

def write_results_df(df, query_id, spark):
    run_ts = F.current_timestamp()
    # 注意：to_json 会将 struct 转 JSON
    payload = (df
               .select(F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("row_json"))
               .withColumn("query_id", F.lit(query_id))
               .withColumn("run_ts", run_ts)
               .select("query_id", "run_ts", "row_json"))
    (payload.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", RESULTS_TABLE)
        .options(**PG_PROPS)
        .mode("append")
        .save())

def main():
    spark = (SparkSession.builder
             .appName("BankingCaseStudy_AllQueries_ToSingleTable")
             .getOrCreate())

    # --- 读取核心表 ---
    customers    = read_table(spark, "customers")
    accounts     = read_table(spark, "accounts")
    transactions = read_table(spark, "transactions")

    # 日期便捷函数
    today      = F.current_date()
    days       = lambda n: F.date_sub(today, n)
    months_ago = lambda n: F.add_months(today, -n)
    years_ago  = lambda n: F.add_months(today, -12*n)

    # ============== 1. Basic ==============

    # 1a) 各客户、账户类型下总余额
    q1a = (accounts
           .groupBy("customer_id","account_type")
           .agg(F.sum("balance").alias("total_balance"))
           .orderBy("customer_id","account_type"))
    write_results_df(q1a, "q1a", spark)

    # 1b) 最近 365 天开新账户
    q1b = (accounts.alias("a")
           .join(customers.alias("c"), "customer_id")
           .where(F.col("a.opening_date") >= days(365))
           .select("c.customer_id","c.first_name","c.last_name","a.account_id","a.opening_date")
           .orderBy(F.col("a.opening_date").desc(),"c.customer_id"))
    write_results_df(q1b, "q1b", spark)

    # 1c) 总余额最高前 5 客户
    tot = accounts.groupBy("customer_id").agg(F.sum("balance").alias("total_balance"))
    q1c = (tot.alias("t")
           .join(customers.alias("c"), "customer_id")
           .select("c.first_name","c.last_name","t.total_balance")
           .orderBy(F.col("t.total_balance").desc())
           .limit(5))
    write_results_df(q1c, "q1c", spark)

    # ============== 2. Transaction Analysis ==============

    # 2a) 过去 30 天 > $500 的取款
    q2a = (transactions.alias("t")
           .join(accounts.alias("a"), "account_id")
           .join(customers.alias("c"), "customer_id")
           .where(
               (F.col("t.transaction_type")=="Withdrawal") &
               (F.col("t.amount") > 500) &
               (F.col("t.transaction_date") >= days(30))
           )
           .select("t.account_id","c.customer_id","c.first_name","c.last_name","t.transaction_date","t.amount")
           .orderBy(F.col("t.transaction_date").desc(),F.col("t.amount").desc()))
    write_results_df(q2a, "q2a", spark)

    # 2b) 最近 6 个月各客户存款总额
    q2b = (transactions.alias("t")
           .join(accounts.alias("a"), "account_id")
           .where(
               (F.col("t.transaction_type")=="Deposit") &
               (F.col("t.transaction_date") >= months_ago(6))
           )
           .groupBy("a.customer_id")
           .agg(F.sum("t.amount").alias("total_deposits"))
           .orderBy("customer_id"))
    write_results_df(q2b, "q2b", spark)

    # 2c) 运行余额（当前余额 - 更晚交易累计）
    signed = (transactions
              .withColumn(
                  "signed_amount",
                  F.when(F.col("transaction_type")=="Deposit", F.col("amount"))
                   .when(F.col("transaction_type").isin("Withdrawal","Payment","Transfer"), -F.col("amount"))
                   .otherwise(F.lit(0.0))
              ))
    desc_win = (W.partitionBy("account_id")
                .orderBy(F.col("transaction_date").desc(), F.col("transaction_id").desc())
                .rowsBetween(W.unboundedPreceding, -1))
    tx_ordered = signed.withColumn("future_sum_signed", F.sum("signed_amount").over(desc_win))
    q2c = (tx_ordered.alias("txo")
           .join(accounts.alias("a"), "account_id")
           .select(
               "txo.account_id","txo.transaction_id","txo.transaction_date",
               "txo.transaction_type","txo.amount",
               (F.col("a.balance") - F.coalesce(F.col("txo.future_sum_signed"), F.lit(0.0))).alias("running_balance")
           )
           .orderBy("account_id","transaction_date","transaction_id"))
    write_results_df(q2c, "q2c", spark)

    # ============== 3. Advanced ==============

    # 3a) 过去一年平均交易额 vs 平均余额
    last_year_tx = (transactions.alias("t")
                    .join(accounts.alias("a"), "account_id")
                    .where(F.col("t.transaction_date") >= years_ago(1))
                    .select(F.col("a.customer_id").alias("customer_id"), F.col("t.amount").alias("amount")))
    avg_tx  = last_year_tx.groupBy("customer_id").agg(F.avg("amount").alias("avg_transaction_amount"))
    avg_bal = accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_balance"))
    q3a = (customers.alias("c")
           .join(avg_tx.alias("atx"), "customer_id", "left")
           .join(avg_bal.alias("ab"), "customer_id", "left")
           .select(
               "c.customer_id",
               F.coalesce(F.col("atx.avg_transaction_amount"), F.lit(0.0)).alias("avg_transaction_amount"),
               F.coalesce(F.col("ab.avg_balance"), F.lit(0.0)).alias("avg_balance")
           )
           .orderBy("customer_id"))
    write_results_df(q3a, "q3a", spark)

    # 3b) 最近 3 个月交易最频繁客户（并列全保留）
    tx3m = (transactions.alias("t")
            .join(accounts.alias("a"), "account_id")
            .where(F.col("t.transaction_date") >= months_ago(3))
            .groupBy("a.customer_id")
            .agg(F.count(F.lit(1)).alias("number_of_transactions")))
    ranked = tx3m.withColumn("rnk", F.dense_rank().over(W.orderBy(F.col("number_of_transactions").desc())))
    q3b = (ranked.alias("r")
           .join(customers.alias("c"), "customer_id")
           .where(F.col("r.rnk")==1)
           .select("c.customer_id","c.first_name","c.last_name","r.number_of_transactions")
           .orderBy("c.customer_id"))
    write_results_df(q3b, "q3b", spark)

    # ============== 4. Data Quality ==============

    # 4a) 账户余额与交易和不匹配
    signed_only = signed.select("account_id","signed_amount")
    calc = signed_only.groupBy("account_id").agg(F.sum("signed_amount").alias("calculated_balance"))
    q4a = (accounts.alias("a")
           .join(calc.alias("c"), "account_id", "left")
           .select(
               "a.account_id",
               F.col("a.balance").alias("account_balance"),
               F.coalesce(F.col("c.calculated_balance"), F.lit(0.0)).alias("calculated_balance")
           )
           .where(F.col("calculated_balance") != F.coalesce(F.col("account_balance"), F.lit(0.0)))
           .orderBy("account_id"))
    write_results_df(q4a, "q4a", spark)

    # 4b) 客户缺失字段
    q4b = (customers
           .withColumn("missing_fields",
               F.array_remove(F.array(
                   F.when(F.col("date_of_birth").isNull(), F.lit("date_of_birth")),
                   F.when(F.trim(F.col("address"))=="", F.lit("address")),
                   F.when(F.trim(F.col("zip"))=="", F.lit("zip"))
               ), F.lit(None)))
           .where(F.size("missing_fields") > 0)
           .select("customer_id","first_name","last_name",
                   F.array_join("missing_fields", ",").alias("missing_fields"))
           .orderBy("customer_id"))
    write_results_df(q4b, "q4b", spark)

    # 4c) 重复账户类型
    cnt = accounts.groupBy("customer_id","account_type").agg(F.count("*").alias("cnt"))
    q4c = (cnt.where(F.col("cnt")>1)
             .select("customer_id","account_type",
                     (F.col("cnt")-F.lit(1)).alias("number_of_duplicates"),
                     F.col("cnt").alias("total_accounts_of_type"))
             .orderBy("customer_id","account_type"))
    write_results_df(q4c, "q4c", spark)

    # 4d) 无效交易类型
    q4d = (transactions
           .where(F.col("transaction_type").isNull() |
                  (~F.col("transaction_type").isin("Deposit","Withdrawal","Payment","Transfer")))
           .select("transaction_id","account_id","transaction_type")
           .orderBy("transaction_id"))
    write_results_df(q4d, "q4d", spark)

    # 4e) 非信用账户负余额
    q4e = (accounts
           .where((F.col("account_type")!="Credit") & (F.col("balance") < 0))
           .select("account_id","customer_id","account_type","balance")
           .orderBy("customer_id","account_id"))
    write_results_df(q4e, "q4e", spark)

    spark.stop()

if __name__ == "__main__":
    main()

