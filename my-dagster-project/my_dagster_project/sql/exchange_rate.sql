MERGE INTO dagster.exchange_rate AS dst
USING (
    SELECT timestamp, date, from_cur, to_cur, rate
    FROM dagster.exchange_rate_staging
) AS src
ON dst.timestamp = src.timestamp
AND dst.date = src.date
AND dst.from_cur = src.from_cur
AND dst.to_cur = src.to_cur
WHEN MATCHED THEN
    UPDATE SET dst.rate = src.rate
WHEN NOT MATCHED THEN
    INSERT (timestamp, date, from_cur, to_cur, rate)
    VALUES (timestamp, date, from_cur, to_cur, rate)