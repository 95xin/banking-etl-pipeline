-- Core tables
CREATE TABLE IF NOT EXISTS customers (
  customer_id   INTEGER PRIMARY KEY,
  first_name    TEXT,
  last_name     TEXT,
  date_of_birth DATE,
  address       TEXT,
  city          TEXT,
  state         TEXT,
  zip           TEXT
);

CREATE TABLE IF NOT EXISTS accounts (
  account_id   INTEGER PRIMARY KEY,
  customer_id  INTEGER NOT NULL REFERENCES customers(customer_id),
  account_type TEXT,
  opening_date DATE,
  balance      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS transactions (
  transaction_id   INTEGER PRIMARY KEY,
  account_id       INTEGER NOT NULL REFERENCES accounts(account_id),
  transaction_date DATE,
  transaction_type TEXT,
  amount           DOUBLE PRECISION,
  description      TEXT
);


/* =========================
   1. Basic Queries
   ========================= */

-- 1a) 
SELECT
  a.customer_id,
  a.account_type,
  SUM(a.balance) AS total_balance
FROM accounts a
GROUP BY a.customer_id, a.account_type
ORDER BY a.customer_id, a.account_type;

-- 1b) 
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  a.account_id,
  a.opening_date
FROM accounts a
JOIN customers c ON c.customer_id = a.customer_id
WHERE a.opening_date >= (CURRENT_DATE - INTERVAL '365 days')
ORDER BY a.opening_date DESC, c.customer_id;

-- 1c) 
WITH tot AS (
  SELECT
    a.customer_id,
    SUM(a.balance) AS total_balance
  FROM accounts a
  GROUP BY a.customer_id
)
SELECT
  c.first_name,
  c.last_name,
  t.total_balance
FROM tot t
JOIN customers c ON c.customer_id = t.customer_id
ORDER BY t.total_balance DESC
limit 5;


/* =========================
   2. Transaction Analysis
   ========================= */

-- 2a) 
SELECT
  t.account_id,
  a.customer_id,
  c.first_name,
  c.last_name,
  t.transaction_date,
  t.amount
FROM transactions t
JOIN accounts a  ON a.account_id  = t.account_id
JOIN customers c ON c.customer_id = a.customer_id
WHERE t.transaction_type = 'Withdrawal'
  AND t.amount > 500
  AND t.transaction_date >= (CURRENT_DATE - INTERVAL '30 days')
ORDER BY t.transaction_date DESC, t.amount DESC;

-- 2b) 
SELECT
  a.customer_id,
  SUM(t.amount) AS total_deposits
FROM transactions t
JOIN accounts a ON a.account_id = t.account_id
WHERE t.transaction_type = 'Deposit'
  AND t.transaction_date >= (CURRENT_DATE - INTERVAL '6 months')
GROUP BY a.customer_id
ORDER BY a.customer_id;

-- 2c) 
WITH tx AS (
  SELECT
    t.account_id,
    t.transaction_id,
    t.transaction_date,
    t.transaction_type,
    t.amount,
    CASE
      WHEN t.transaction_type = 'Deposit' THEN  t.amount
      WHEN t.transaction_type IN ('Withdrawal','Payment','Transfer') THEN -t.amount
      ELSE 0
    END AS signed_amount
  FROM transactions t
),
tx_ordered AS (
  SELECT
    tx.*,
    SUM(tx.signed_amount) OVER (
      PARTITION BY tx.account_id
      ORDER BY tx.transaction_date DESC, tx.transaction_id DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS future_sum_signed
  FROM tx
)
SELECT
  txo.account_id,
  txo.transaction_id,
  txo.transaction_date,
  txo.transaction_type,
  txo.amount,
  (a.balance - COALESCE(txo.future_sum_signed, 0)) AS running_balance
FROM tx_ordered txo
JOIN accounts a ON a.account_id = txo.account_id
ORDER BY txo.account_id, txo.transaction_date, txo.transaction_id;


/* =========================
   3. Advanced Queries
   ========================= */

-- 3a) 
WITH last_year_tx AS (
  SELECT
    a.customer_id,
    t.amount
  FROM transactions t
  JOIN accounts a ON a.account_id = t.account_id
  WHERE t.transaction_date >= (CURRENT_DATE - INTERVAL '1 year')
),
avg_tx AS (
  SELECT
    customer_id,
    AVG(amount) AS avg_transaction_amount
  FROM last_year_tx
  GROUP BY customer_id
),
avg_bal AS (
  SELECT
    a.customer_id,
    AVG(a.balance) AS avg_balance
  FROM accounts a
  GROUP BY a.customer_id
)
SELECT
  c.customer_id,
  COALESCE(atx.avg_transaction_amount, 0) AS avg_transaction_amount,
  COALESCE(ab.avg_balance, 0)            AS avg_balance
FROM customers c
LEFT JOIN avg_tx  atx ON atx.customer_id = c.customer_id
LEFT JOIN avg_bal ab  ON ab.customer_id  = c.customer_id
ORDER BY c.customer_id;

-- 3b) 
WITH tx3m AS (
  SELECT
    a.customer_id,
    COUNT(*) AS number_of_transactions
  FROM transactions t
  JOIN accounts a ON a.account_id = t.account_id
  WHERE t.transaction_date >= (CURRENT_DATE - INTERVAL '3 months')
  GROUP BY a.customer_id
),
ranked AS (
  SELECT
    customer_id,
    number_of_transactions,
    DENSE_RANK() OVER (ORDER BY number_of_transactions DESC) AS rnk
  FROM tx3m
)
SELECT
  c.customer_id, c.first_name, c.last_name, r.number_of_transactions
FROM ranked r
JOIN customers c ON c.customer_id = r.customer_id
WHERE r.rnk = 1
ORDER BY c.customer_id;

  
/* =========================
   4. Data Quality Checks
   ========================= */

-- 4a) 
WITH signed AS (
  SELECT
    t.account_id,
    CASE
      WHEN t.transaction_type = 'Deposit' THEN  t.amount
      WHEN t.transaction_type IN ('Withdrawal','Payment','Transfer') THEN -t.amount
      ELSE 0
    END AS signed_amount
  FROM transactions t
),
calc AS (
  SELECT
    account_id,
    COALESCE(SUM(signed_amount),0) AS calculated_balance
  FROM signed
  GROUP BY account_id
)
SELECT
  a.account_id,
  a.balance        AS account_balance,
  c.calculated_balance
FROM accounts a
LEFT JOIN calc c ON c.account_id = a.account_id
WHERE COALESCE(c.calculated_balance,0) <> COALESCE(a.balance,0)
ORDER BY a.account_id;

-- 4b) 
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  TRIM(BOTH ',' FROM
      (CASE WHEN c.date_of_birth IS NULL THEN 'date_of_birth,' ELSE '' END) ||
      (CASE WHEN NULLIF(BTRIM(c.address),'') IS NULL THEN 'address,' ELSE '' END) ||
      (CASE WHEN NULLIF(BTRIM(c.zip),    '') IS NULL THEN 'zip,'     ELSE '' END)
  ) AS missing_fields
FROM customers c
WHERE (c.date_of_birth IS NULL)
   OR (NULLIF(BTRIM(c.address),'') IS NULL)
   OR (NULLIF(BTRIM(c.zip),    '') IS NULL)
ORDER BY c.customer_id;

-- 4c) 
WITH cnt AS (
  SELECT
    customer_id,
    account_type,
    COUNT(*) AS cnt
  FROM accounts
  GROUP BY customer_id, account_type
)
SELECT
  customer_id,
  account_type,
  (cnt - 1) AS number_of_duplicates,
  cnt       AS total_accounts_of_type
FROM cnt
WHERE cnt > 1
ORDER BY customer_id, account_type;

-- 4d) 
SELECT
  t.transaction_id,
  t.account_id,
  t.transaction_type
FROM transactions t
WHERE t.transaction_type IS NULL
   OR t.transaction_type NOT IN ('Deposit','Withdrawal','Payment','Transfer')
ORDER BY t.transaction_id;

-- 4e) 
SELECT
  a.account_id,
  a.customer_id,
  a.account_type,
  a.balance
FROM accounts a
WHERE a.account_type <> 'Credit'
  AND a.balance < 0
ORDER BY a.customer_id, a.account_id;