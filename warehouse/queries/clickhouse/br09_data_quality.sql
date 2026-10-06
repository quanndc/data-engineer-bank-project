-- BR9 (Data team): results of the latest data quality run.
SELECT check_id,
       characteristic,
       metric,
       expected,
       observed,
       multiIf(passed IS NULL, 'INFO', passed = 1, 'PASS', 'FAIL') AS status,
       run_ts
FROM dq.audit
WHERE run_id = (SELECT argMax(run_id, run_ts) FROM dq.audit)
ORDER BY check_id
