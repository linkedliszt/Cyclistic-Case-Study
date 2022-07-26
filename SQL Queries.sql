--Execute this code 12 times to create tables for each dataset, then proceed to import the 12 months of data

DROP TABLE IF EXISTS t_2022_05;
CREATE TABLE t_2022_05(
    ride_id text,
    rideable_type text,
    started_at timestamp,
    ended_at timestamp,
    start_station_name text,
    start_station_id text,
    end_station_name text,
    end_station_id text,
    start_lat real,
    start_lng real,
    end_lat real,
    end_lng real,
    member_casual text
    );


--Combine 12 months worth of data into a single table named, combined

CREATE TABLE AS combined(
    SELECT * FROM t_2022_05
    UNION
    SELECT * FROM t_2022_04
    UNION
    SELECT * FROM t_2022_03
    UNION
    SELECT * FROM t_2022_02
    UNION
    SELECT * FROM t_2022_01
    UNION
    SELECT * FROM t_2021_12
    UNION
    SELECT * FROM t_2021_11
    UNION
    SELECT * FROM t_2021_10
    UNION
    SELECT * FROM t_2021_09
    UNION
    SELECT * FROM t_2021_08
    UNION
    SELECT * FROM t_2021_07
    UNION
    SELECT * FROM t_2021_06
);


---------------------------------------------CLEANING DATA-----------------------------------------


--Checking for duplicates

SELECT 
    COUNT(DISTINCT(ride_id)),
    COUNT(ride_id)
FROM combined_raw


--Removing null values, 1193477 rows deleted, 4667299 left

DELETE FROM combined
WHERE
    ride_id IS NULL
    OR
    rideable_type IS NULL
    OR
    started_at IS NULL
    OR
    ended_at IS NULL
    OR
    start_station_name IS NULL
    OR
    start_station_id IS NULL
    OR
    end_station_name IS NULL
    OR
    end_station_id IS NULL
    OR
    start_lat IS NULL
    OR
    start_lng IS NULL
    OR
    end_lat IS NULL
    OR
    end_lng IS NULL
    OR
    member_casual IS NULL




--Trimming all data values which are text

UPDATE combined
SET 
    ride_id = TRIM(ride_id),
    rideable_type = TRIM(rideable_type),
    start_station_name = TRIM(start_station_name),
    start_station_id = TRIM(start_station_id),
    end_station_name = TRIM(end_station_name),
    end_station_id = TRIM(end_station_id),
    member_casual = TRIM(member_casual)




--Including day_of_week, ride_length, hour_of_day, month columns


ALTER TABLE combined
ADD COLUMN ride_length numeric,
ADD COLUMN day_of_week VARCHAR
ADD COLUMN month INT,
ADD COLUMN hour_of_day INT
    


UPDATE combined
SET day_of_week = to_char(CAST(started_at as DATE), 'Day'),
    ride_length = ROUND(EXTRACT(epoch FROM ended_at-started_at)/60, 2),
    month = EXTRACT(MONTH FROM started_at),
    hour_of_day = EXTRACT(HOUR FROM started_at)


--Removing invalid data

DELETE FROM combined
WHERE ride_length <=0 OR ride_length>=1440





----------------------------------------------ANALYSIS-----------------------------------------------

SELECT 
    member_casual,
    rideable_type,
    month,
    day_of_week,
    hour_of_day,
    ROUND(AVG(ride_length),1) AS av_ride_length,
    COUNT(*) AS TotalCount
FROM
    combined
GROUP BY
    member_casual,
    rideable_type,
    month,
    day_of_week,
    hour_of_day



--EXTRACTING START AND END LOCATIONS

--Start locations for casuals

SELECT
    member_casual,
    start_station_name,
    ROUND(AVG(CAST(start_lat AS decimal)),4) AS start_lat,
    ROUND(AVG(CAST(start_lng AS decimal)),4) AS start_lng,
    COUNT(*) AS TotalCount
    
FROM
    combined
WHERE 
    member_casual = 'casual'
GROUP BY
    member_casual,
    start_station_name
    



--Start locations for members


SELECT
    member_casual,
    start_station_name,
    ROUND(AVG(CAST(start_lat AS decimal)),4) AS start_lat,
    ROUND(AVG(CAST(start_lng AS decimal)),4) AS start_lng,
    COUNT(*) AS TotalCount
    
FROM
    combined
WHERE 
    member_casual = 'members'
GROUP BY
    member_casual,
    start_station_name
   



--End locations for casuals

SELECT
    member_casual,
    end_station_name,
    ROUND(AVG(CAST(end_lat AS decimal)),4) AS end_lat,
    ROUND(AVG(CAST(end_lng AS decimal)),4) AS end_lng,
    COUNT(*) AS TotalCount
    
FROM
    combined
WHERE 
    member_casual = 'casual'
GROUP BY
    member_casual,
    end_station_name



--End locations for members

SELECT
    member_casual,
    end_station_name,
    ROUND(AVG(CAST(end_lat AS decimal)),4) AS end_lat,
    ROUND(AVG(CAST(end_lng AS decimal)),4) AS end_lng,
    COUNT(*) AS TotalCount
    
FROM
    combined
WHERE 
    member_casual = 'member'
GROUP BY
    member_casual,
    end_station_name