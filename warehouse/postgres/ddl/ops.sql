-- E. Operational store (PostgreSQL), schema ops.
-- Sheet: "PostgreSQL: lô/lần chạy, lỗi, cảnh báo, ghi chú và thông tin bản sao lưu".
-- It holds what the pipeline (C) writes while it runs and what the application (F) writes back
-- (alert status, notes, backup operations). It never stores warehouse data or backup copies.
-- Idempotent: objects are created only if missing.

CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------- batches and runs (BR06)
CREATE TABLE IF NOT EXISTS ops.batch (
  batch_id       text PRIMARY KEY,               -- e.g. HIST_1993_01, KB05_ERRORS
  batch_type     text NOT NULL CHECK (batch_type IN ('HISTORICAL', 'SCENARIO')),
  scenario_code  text,                           -- KB01..KB08 for scenario batches
  description    text,
  period_from    date,
  period_to      date,
  lake_path      text,                           -- input snapshot in the Data Lake (B)
  received_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.pipeline_run (
  run_id         bigserial PRIMARY KEY,
  batch_id       text NOT NULL REFERENCES ops.batch,
  run_kind       text NOT NULL DEFAULT 'RUN' CHECK (run_kind IN ('RUN', 'RERUN')),
  triggered_by   text NOT NULL DEFAULT 'cli',    -- cli | app
  status         text NOT NULL DEFAULT 'RUNNING'
                 CHECK (status IN ('RUNNING', 'SUCCEEDED', 'SUCCEEDED_WITH_REJECTS', 'FAILED')),
  started_at     timestamptz NOT NULL DEFAULT now(),
  finished_at    timestamptz,
  rows_received  bigint,
  rows_loaded    bigint,
  rows_rejected  bigint,
  failed_step    text,
  error_message  text
);
CREATE INDEX IF NOT EXISTS pipeline_run_batch_idx ON ops.pipeline_run (batch_id, started_at DESC);

CREATE TABLE IF NOT EXISTS ops.pipeline_step (
  step_id        bigserial PRIMARY KEY,
  run_id         bigint NOT NULL REFERENCES ops.pipeline_run ON DELETE CASCADE,
  step_order     smallint NOT NULL,
  step_name      text NOT NULL,                  -- ingest, validate, standardize, ...
  status         text NOT NULL DEFAULT 'RUNNING'
                 CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')),
  started_at     timestamptz NOT NULL DEFAULT now(),
  finished_at    timestamptz,
  rows_in        bigint,
  rows_out       bigint,
  rows_rejected  bigint,
  error_message  text,
  UNIQUE (run_id, step_order)
);

-- ---------------------------------------------------------------- data control (BR05)
CREATE TABLE IF NOT EXISTS ops.dq_rule (
  rule_code      text PRIMARY KEY,               -- V1..V6 (decision Q-G)
  description    text NOT NULL,
  severity       text NOT NULL DEFAULT 'REJECT' CHECK (severity IN ('REJECT', 'WARN'))
);

CREATE TABLE IF NOT EXISTS ops.rejected_record (
  reject_id      bigserial PRIMARY KEY,
  run_id         bigint NOT NULL REFERENCES ops.pipeline_run ON DELETE CASCADE,
  batch_id       text NOT NULL REFERENCES ops.batch,
  source_table   text NOT NULL,                  -- trans, account, ...
  source_key     text,                           -- e.g. trans_id as read
  rule_code      text NOT NULL REFERENCES ops.dq_rule,
  reason         text NOT NULL,
  record         jsonb NOT NULL,                 -- the rejected source row, as read
  rejected_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rejected_record_run_idx ON ops.rejected_record (run_id, rule_code);

-- ---------------------------------------------------------------- alerts (BR03, BR04)
CREATE TABLE IF NOT EXISTS ops.alert_rule (
  rule_code      text PRIMARY KEY,               -- R1, R2 (decision Q-F)
  description    text NOT NULL,
  parameters     jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ops.alert (
  alert_id         bigserial PRIMARY KEY,
  rule_code        text NOT NULL REFERENCES ops.alert_rule,
  account_id       integer NOT NULL,             -- source account id (as shown to the analyst)
  period_start     date NOT NULL,
  period_end       date NOT NULL,
  reason           text NOT NULL,
  evidence         jsonb NOT NULL,               -- figures the rule used (sheet: "số liệu làm căn cứ")
  transaction_ids  bigint[] NOT NULL DEFAULT '{}',
  run_id           bigint REFERENCES ops.pipeline_run,
  batch_id         text REFERENCES ops.batch,
  detected_at      timestamptz NOT NULL DEFAULT now(),
  status           text NOT NULL DEFAULT 'NEW'
                   CHECK (status IN ('NEW', 'IN_REVIEW', 'RESOLVED')),   -- chưa xem / đang xem xét / đã xử lý
  status_changed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (account_id, rule_code, period_start)   -- one alert per account, rule and period (no duplicates)
);
CREATE INDEX IF NOT EXISTS alert_status_idx ON ops.alert (status, detected_at DESC);

CREATE TABLE IF NOT EXISTS ops.alert_action (
  action_id      bigserial PRIMARY KEY,
  alert_id       bigint NOT NULL REFERENCES ops.alert ON DELETE CASCADE,
  from_status    text NOT NULL,
  to_status      text NOT NULL CHECK (to_status IN ('NEW', 'IN_REVIEW', 'RESOLVED')),
  note           text,
  acted_by       text NOT NULL DEFAULT 'analyst',   -- no login in the demo (sheet: chưa phân quyền)
  acted_at       timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- backup and restore (BR07)
CREATE TABLE IF NOT EXISTS ops.backup_catalog (
  backup_id      text PRIMARY KEY,               -- name of the backup on the ClickHouse 'backups' disk
  created_at     timestamptz NOT NULL DEFAULT now(),
  status         text NOT NULL CHECK (status IN ('CREATING', 'AVAILABLE', 'FAILED')),
  tables         text[] NOT NULL,                -- the 6 warehouse tables
  row_counts     jsonb,                          -- rows per table at backup time
  totals         jsonb,                          -- money in / out, by tier, SCD versions: for reconciliation
  note           text
);

CREATE TABLE IF NOT EXISTS ops.backup_operation (
  op_id          bigserial PRIMARY KEY,
  backup_id      text NOT NULL REFERENCES ops.backup_catalog,
  op_type        text NOT NULL CHECK (op_type IN ('BACKUP', 'RESTORE')),
  status         text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
  started_at     timestamptz NOT NULL DEFAULT now(),
  finished_at    timestamptz,
  result         jsonb,                          -- before/after figures of the reconciliation
  error_message  text
);

-- ---------------------------------------------------------------- reference rows
INSERT INTO ops.dq_rule (rule_code, description) VALUES
  ('V1', 'Thiếu trường bắt buộc (trans_id, account_id, date, type, amount)'),
  ('V2', 'Tài khoản không tồn tại trong kho'),
  ('V3', 'Số tiền âm hoặc không phải số'),
  ('V4', 'Ngày không hợp lệ hoặc nằm ngoài lịch'),
  ('V5', 'Mã loại giao dịch lạ (không phải PRIJEM / VYDAJ / VYBER)'),
  ('V6', 'Trùng transaction_id trong lô hoặc đã có trong kho'),
  ('V7', 'Không có phiên bản chủ sở hữu (dim_customer) hiệu lực tại ngày giao dịch')
ON CONFLICT (rule_code) DO NOTHING;

INSERT INTO ops.alert_rule (rule_code, description, parameters) VALUES
  ('R1', 'Tiền ra tăng mạnh: tiền ra trong tháng > 3 lần trung bình 6 tháng trước và > 50.000 CZK',
   '{"multiplier": 3, "lookback_months": 6, "min_outflow_czk": 50000}'),
  ('R2', 'Tài khoản ngủ đông hoạt động lại: không giao dịch >= 180 ngày rồi có giao dịch ra',
   '{"dormant_days": 180}')
ON CONFLICT (rule_code) DO NOTHING;
