SELECT *
FROM users;

SELECT * 
FROM events
LIMIT 50;


--grouping by gender
SELECT gender, COUNT(gender) AS count_gender
FROM users
GROUP BY gender
ORDER BY count_gender DESC;


--grouping by age range
SELECT CASE
           WHEN age >= 20 AND age < 30 THEN '20-29'
           WHEN age >= 30 AND age < 40 THEN '30-39'
           WHEN age >= 40 AND age < 50 THEN '40-49'
           WHEN age >= 50 AND age < 60 THEN '50-59'
           WHEN age >= 60 AND age < 70 THEN '60-70'
           ELSE '70+'
           END AS age_range,
           COUNT(*) AS age_count
FROM users
GROUP BY age_range
ORDER BY age_range;


--grouping by events
SELECT CASE 
          WHEN event = 'action_1' THEN 'right swipe'
          WHEN event = 'action_2' THEN 'left swipe'
          ELSE 'app start'
          END AS event,
          COUNT(*) AS event_count 
FROM events
GROUP BY event
ORDER BY event_count DESC;


--average left and right swipes per gender
SELECT gender,
       event,
       AVG(event_count) AS average_swipes
FROM       
    (SELECT gender,
           event,
           event_count
    FROM       
        (SELECT events.user_id,
                gender, 
                CASE 
                    WHEN event = 'action_1' THEN 'right swipe'
                    WHEN event = 'action_2' THEN 'left swipe'
                    ELSE 'app start'
                    END AS event,
                    COUNT(*) AS event_count 
          FROM events
          JOIN users ON events.user_id = users.user_id
          GROUP BY events.user_id, gender, event
          ORDER BY events.user_id, event_count DESC) AS first_query
     WHERE event != 'app start') AS second_query
GROUP BY gender, event;


--average number of days active
SELECT gender,
       AVG(num_days_active) AS avg_days_active
FROM
      (SELECT user_id,
              gender,
              COUNT(date_active) OVER (PARTITION BY(user_id) ORDER BY user_id) AS num_days_active  
        FROM
              (SELECT users.user_id, 
                      gender,
                      DATE(active_date) AS date_active,
                      CASE 
                          WHEN event = 'action_1' THEN 'right swipe'
                          WHEN event = 'action_2' THEN 'left swipe'
                          ELSE 'app start'
                          END AS event      
              FROM users
              JOIN events
              ON users.user_id = events.user_id
              WHERE event != 'app_start'
              ORDER BY users.user_id) AS first_query
        GROUP BY user_id, gender, date_active) AS second_query
GROUP BY gender
ORDER BY gender;


--average number of events per session and average session length. An event is a left or right swipe
--creating multiple tables for this query as this will make the code clearer to read and make each section run faster. Tables will be dropped at the end
CREATE TABLE users_events AS
SELECT users.user_id,
       gender,
       active_date,
       active_date :: time AS active_time,
       CASE WHEN event = 'action_1' OR event = 'action_2' THEN 'left or right swipe' ELSE 'app start' END AS event
FROM users
JOIN events
ON users.user_id = events.user_id;

--creating a second table for next part of query
CREATE TABLE users_events2 AS
SELECT user_id,
       gender,
       active_date,
       active_time,
       event,
       --partitioning over app starts and left and right swipes by user
       RANK() OVER (PARTITION BY user_id, event ORDER BY user_id, active_date) AS event_count,
       --calculating time difference and casting time difference to minutes/seconds
       active_time - LAG(active_time) OVER (PARTITION BY user_id ORDER BY user_id, active_date) :: INTERVAL second AS time_diff 
FROM users_events;

--creating table which adds a column to count number of sessions
CREATE TABLE users_events3 AS
SELECT user_id,
       gender,
       active_date,
       active_time,
       event,
       event_count,
       --counting the number of sessions
       SUM(CASE WHEN event = 'app start' THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY user_id, active_date) AS session_count,
       time_diff
FROM users_events2;

--create table to show events per session and time per session
CREATE TABLE users_events4 AS
SELECT user_id,
       gender,
       active_date,
       event,
       event_count,
       session_count,
       time_diff,
       --counting number of events per sesson
       COUNT(session_count) OVER (PARTITION BY user_id, session_count) AS events_per_session,
       --summing time per session
       SUM(time_diff) OVER (PARTITION BY user_id, session_count) AS minutes_per_session
FROM users_events3
WHERE event != 'app start'
GROUP BY user_id, gender, active_date, active_time, event, event_count, session_count, time_diff
ORDER BY user_id, active_date, event_count, session_count;

--can use a CTE for the final part of the query - first extract minutes from the timestamp
WITH users_events5 AS
(SELECT user_id,
        gender,
        active_date,
        events_per_session,
        session_count,
        EXTRACT(minute FROM minutes_per_session) AS session_time
FROM users_events4
ORDER BY user_id, active_date, session_count),

--next CTE calculates average events and minutes per user
users_events6 AS
(SELECT user_id,
       gender,
       AVG(events_per_session) OVER (PARTITION BY user_id) AS avg_events_user,
       AVG(session_time) OVER (PARTITION BY user_id) AS avg_minutes_user
FROM users_events5
GROUP BY user_id, gender, events_per_session, session_time
ORDER BY user_id)

--now calculate final average
SELECT gender,
       AVG(avg_events_user) AS avg_events_per_session,
       AVG(avg_minutes_user) AS avg_minutes_per_session
FROM users_events6
GROUP BY gender;

--can now drop the tables created as no longer needed
DROP TABLE users_events;
DROP TABLE users_events2;
DROP TABLE users_events3;
DROP TABLE users_events4;
