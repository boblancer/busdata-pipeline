-- 1. How many breadcrumb reading events occurred on January 1, 2023?
SELECT COUNT(*) 
FROM BreadCrumb 
WHERE tstamp::date = '2023-01-01';

-- 2. How many breadcrumb reading events occurred on January 2, 2023?
SELECT COUNT(*) 
FROM BreadCrumb 
WHERE tstamp::date = '2023-01-02';

-- 3. On average, how many breadcrumb readings are collected on each day of the week?
SELECT 
    CASE EXTRACT(DOW FROM tstamp)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week,
    COUNT(*) / COUNT(DISTINCT tstamp::date) AS avg_readings_per_day
FROM BreadCrumb
GROUP BY EXTRACT(DOW FROM tstamp)
ORDER BY EXTRACT(DOW FROM tstamp);

-- 4. List the TriMet trips that traveled a section of I-205 between SE Division and SE Powell on January 1, 2023
SELECT DISTINCT trip_id
FROM BreadCrumb
WHERE 
    tstamp::date = '2023-01-01'
    AND latitude BETWEEN 45.497805 AND 45.504025
    AND longitude BETWEEN -122.566576 AND -122.563187
ORDER BY trip_id;

-- 5. List all breadcrumb readings on a section of US-26 west side of the tunnel during specific times
sql-- For Mondays between 4pm and 6pm
SELECT bc.*, t.vehicle_id
FROM BreadCrumb bc
JOIN Trip t ON bc.trip_id = t.trip_id
WHERE 
    latitude BETWEEN 45.506022 AND 45.516636
    AND longitude BETWEEN -122.711662 AND -122.700316
    AND EXTRACT(DOW FROM tstamp) = 1  -- 1 is Monday
    AND EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 16 AND 17  -- 4pm to 6pm (hour 16 and 17)
ORDER BY tstamp;

-- For Sundays between 6am and 8am
SELECT bc.*, t.vehicle_id
FROM BreadCrumb bc
JOIN Trip t ON bc.trip_id = t.trip_id
WHERE 
    latitude BETWEEN 45.506022 AND 45.516636
    AND longitude BETWEEN -122.711662 AND -122.700316
    AND EXTRACT(DOW FROM tstamp) = 0  -- 0 is Sunday
    AND EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 6 AND 7  -- 6am to 8am (hour 6 and 7)
ORDER BY tstamp;

-- To compare, we can count the number of readings for each period:
SELECT 
    'Monday 4pm-6pm' AS time_period,
    COUNT(*) AS reading_count
FROM BreadCrumb
WHERE 
    latitude BETWEEN 45.506022 AND 45.516636
    AND longitude BETWEEN -122.711662 AND -122.700316
    AND EXTRACT(DOW FROM tstamp) = 1  -- 1 is Monday
    AND EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 16 AND 17  -- 4pm to 6pm
UNION ALL
SELECT 
    'Sunday 6am-8am' AS time_period,
    COUNT(*) AS reading_count
FROM BreadCrumb
WHERE 
    latitude BETWEEN 45.506022 AND 45.516636
    AND longitude BETWEEN -122.711662 AND -122.700316
    AND EXTRACT(DOW FROM tstamp) = 0  -- 0 is Sunday
    AND EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 6 AND 7;  -- 6am to 8am

-- 6. What is the maximum speed reached by any bus in the system?
SELECT MAX(speed) AS max_speed
FROM BreadCrumb
WHERE speed IS NOT NULL;

-- 7. List all speeds and counts of vehicles that move precisely at that speed
SELECT 
    speed,
    COUNT(DISTINCT t.vehicle_id) AS vehicle_count
FROM BreadCrumb bc
JOIN Trip t ON bc.trip_id = t.trip_id
WHERE speed IS NOT NULL
GROUP BY speed
ORDER BY vehicle_count DESC, speed;

-- 8. Which is the longest (in terms of time) trip of all trips in the data?
SELECT 
    trip_id,
    MIN(tstamp) AS start_time,
    MAX(tstamp) AS end_time,
    MAX(tstamp) - MIN(tstamp) AS trip_duration
FROM BreadCrumb
GROUP BY trip_id
ORDER BY trip_duration DESC
LIMIT 1;

-- 9. Comparing breadcrumbs between different days (non-holiday Wednesday, Saturday, and holiday)
sql-- Count breadcrumbs for a non-holiday Wednesday (you'll need to identify a specific date)
SELECT 
    'Non-holiday Wednesday' AS day_type,
    COUNT(*) AS breadcrumb_count
FROM BreadCrumb
WHERE tstamp::date = '2023-01-04'  -- Adjust to a specific non-holiday Wednesday
UNION ALL
-- Count breadcrumbs for a non-holiday Saturday
SELECT 
    'Non-holiday Saturday' AS day_type,
    COUNT(*) AS breadcrumb_count
FROM BreadCrumb
WHERE tstamp::date = '2023-01-07'  -- Adjust to a specific non-holiday Saturday
UNION ALL
-- Count breadcrumbs for a holiday (e.g., New Year's Day)
SELECT 
    'Holiday (New Year''s Day)' AS day_type,
    COUNT(*) AS breadcrumb_count
FROM BreadCrumb
WHERE tstamp::date = '2023-01-01';  -- New Year's Day

-- 10. Three new interesting questions:
-- 10.1. What is the average speed of buses during rush hour (7-9am and 4-6pm) compared to off-peak hours?
sql-- Average speed during rush hours
SELECT 
    'Rush Hour (7-9am, 4-6pm)' AS time_period,
    AVG(speed) AS avg_speed,
    COUNT(*) AS reading_count
FROM BreadCrumb
WHERE 
    speed IS NOT NULL AND
    (
        (EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 7 AND 8) OR  -- 7-9am
        (EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 16 AND 17)    -- 4-6pm
    )
UNION ALL
-- Average speed during off-peak hours
SELECT 
    'Off-Peak Hours' AS time_period,
    AVG(speed) AS avg_speed,
    COUNT(*) AS reading_count
FROM BreadCrumb
WHERE 
    speed IS NOT NULL AND
    NOT (
        (EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 7 AND 8) OR  -- 7-9am
        (EXTRACT(HOUR FROM tstamp AT TIME ZONE 'UTC') BETWEEN 16 AND 17)    -- 4-6pm
    );

sql-- 10.2. Which 5 bus trips have the highest average speeds, and where are they primarily operating?
SELECT 
    bc.trip_id,
    AVG(bc.speed) AS avg_speed,
    COUNT(*) AS breadcrumb_count,
    AVG(bc.latitude) AS avg_latitude,
    AVG(bc.longitude) AS avg_longitude
FROM BreadCrumb bc
WHERE 
    bc.speed IS NOT NULL AND
    bc.trip_id IS NOT NULL
GROUP BY bc.trip_id
ORDER BY avg_speed DESC
LIMIT 5;

-- 10.3. How does bus density vary across the city?
-- Bus density by hour of day
SELECT 
    CASE 
        WHEN latitude >= 45.52 AND longitude >= -122.66 THEN 'Northeast'
        WHEN latitude >= 45.52 AND longitude < -122.66 THEN 'Northwest'
        WHEN latitude < 45.52 AND longitude >= -122.66 THEN 'Southeast'
        WHEN latitude < 45.52 AND longitude < -122.66 THEN 'Southwest'
    END AS city_quadrant,
    COUNT(*) AS reading_count,
    COUNT(DISTINCT t.vehicle_id) AS active_buses
FROM BreadCrumb bc
JOIN Trip t ON bc.trip_id = t.trip_id
GROUP BY 
    CASE 
        WHEN latitude >= 45.52 AND longitude >= -122.66 THEN 'Northeast'
        WHEN latitude >= 45.52 AND longitude < -122.66 THEN 'Northwest'
        WHEN latitude < 45.52 AND longitude >= -122.66 THEN 'Southeast'
        WHEN latitude < 45.52 AND longitude < -122.66 THEN 'Southwest'
    END
ORDER BY city_quadrant;