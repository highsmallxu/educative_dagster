MERGE INTO dagster.exchange_rate_report AS dst
USING (
    SELECT date, from_cur, to_cur, AVG(rate) as avg_rate
    FROM dagster.exchange_rate
    GROUP BY 1,2,3
) AS src
ON dst.date = src.date
AND dst.from_cur = src.from_cur
AND dst.to_cur = src.to_cur
WHEN MATCHED THEN
    UPDATE SET dst.avg_rate = src.avg_rate
WHEN NOT MATCHED THEN
    INSERT (date, from_cur, to_cur, avg_rate)
    VALUES (date, from_cur, to_cur, avg_rate)