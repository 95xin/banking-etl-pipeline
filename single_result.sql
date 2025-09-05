-- 单一结果表，用于承载所有查询的每一行结果
CREATE TABLE IF NOT EXISTS case_results (
  query_id TEXT NOT NULL,
  run_ts   TIMESTAMP NOT NULL,
  row_json TEXT NOT NULL
);


CREATE INDEX IF NOT EXISTS idx_case_results_query_ts ON case_results(query_id, run_ts);




SELECT row_json
FROM case_results
WHERE query_id = 'q1a'
ORDER BY run_ts DESC
LIMIT 50;


SELECT
  (row_json::jsonb)->>'customer_id'     AS customer_id,
  (row_json::jsonb)->>'account_type'    AS account_type,
  ((row_json::jsonb)->>'total_balance')::numeric AS total_balance
FROM case_results
WHERE query_id='q1a'
ORDER BY total_balance DESC;
