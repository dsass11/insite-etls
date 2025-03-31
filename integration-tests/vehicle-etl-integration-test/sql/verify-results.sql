SHOW DATABASES;
USE vehicle_datalake;
SHOW TABLES;
-- Try to select from the input table first to verify it exists and has data
SELECT COUNT(*) FROM events;
-- Then try to check if the output table exists but might not have data yet
DESCRIBE processed_vehicles;