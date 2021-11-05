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
       AVG(num_days_active) AS avg_active_days
FROM
        (SELECT DISTINCT user_id,
              gender,
              MAX(active_days) OVER (PARTITION BY user_id ORDER BY user_id) AS num_days_active
        FROM
              (SELECT U.user_id,
                      U.gender,
                      DATE(E.active_date) AS active_day,
                      E.event,
                      DENSE_RANK() OVER (PARTITION BY U.user_id ORDER BY DATE(E.active_date)) AS active_days
                FROM users U
                INNER JOIN events E
                ON U.user_id = E.user_id) A 
        GROUP BY user_id, gender, active_days
        ORDER BY user_id) B
GROUP BY gender;

--average number of events per session and average session length. An event is a left or right swipe
--creating multiple tables for this query as this will make the code clearer to read and make each section run faster. Tables will be dropped at the end
CREATE TABLE users_events AS
SELECT users.user_id,
       gender,
       active_date,
       active_date :: time AS active_time,
       CASE WHEN event = 'action_1' OR event = 'action_2' THEN 'left or right swipe' ELSE 'app start' END AS event
FROM users
INNER JOIN events
ON users.user_id = events.user_id
ORDER BY user_id, active_date;

SELECT *
FROM users_events;

---second table adds a column showing the event count, the next event the user undertakes and also calculates the time between events
CREATE TABLE users_events2 AS
SELECT user_id,
       gender,
       active_date,
       active_time,
       event,
       --partitioning over user_id and event_type to get event_count per user
       RANK() OVER (PARTITION BY user_id, event ORDER BY user_id, active_date) AS event_count,
       --using LEAD to get users' next event in a separate column
       LEAD(event) OVER (PARTITION BY user_id ORDER BY user_id, active_date) AS next_event,
       --using LAG to get time difference between each event
       active_time - LAG(active_time) OVER (PARTITION BY user_id ORDER BY user_id, active_date) :: INTERVAL second AS time_diff
FROM users_events
GROUP BY user_id, gender, active_date, event, active_time
ORDER BY user_id, active_date; 

SELECT *
FROM users_events2;

--third table adds three new columns - a session count for each user, a count for actions in each session and the time between each action
CREATE TABLE users_events3 AS
SELECT user_id,
       gender,
       active_date,
       event,
       event_count,
       next_event,
       --CASE statement counts each instance where an app start is followed by an action - this counts the number of sessions
       SUM(CASE WHEN event = 'app start' AND next_event = 'left or right swipe' THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY user_id, active_date) AS session_count,
       --only want the actions included and not the app starts
       CASE WHEN event = 'left or right swipe' THEN 1 ELSE NULL END AS active_event_count,
       time_diff,
       --only want the time difference for when the user was active and not the time between one session and when the user next starts the app
       CASE WHEN event = 'app start' THEN NULL ELSE time_diff END AS active_session_time
FROM users_events2;

SELECT *
FROM users_events3;

--fourth table is very similar to users_events_2 with just one change to the time column
CREATE TABLE users_events4 AS
SELECT user_id,
        gender,
        active_date,
        event,
        event_count,
        next_event,
        session_count,
        active_event_count,
        active_session_time,
        --using epoch to get the time in minutes - was previously in seconds as this made it easier to check the time difference was correct when creating the code
        EXTRACT('epoch' FROM active_session_time :: INTERVAL) / 60 AS minutes
FROM users_events3;

SELECT *
FROM users_events4;

--using CTEs for the last part of the code - the CTE calculates number of events and minutes per user per session and adds a row count
WITH users_session_data AS
(SELECT user_id,
       gender,
       session_count,
       --summing number of events for each user in each session - adding the range between clause means it sums the whole window rather than a running total
       SUM(active_event_count) OVER (PARTITION BY user_id, session_count ORDER BY user_id, active_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_events_per_session,
       --summing minutes for each user for each session - adding the range between clause means it sums the whole window rather than a running total
       SUM(minutes) OVER (PARTITION BY user_id, session_count ORDER BY user_id, active_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_minutes_per_session,
       --adding a row count for each user id/session so we can retrieve just one row for each user session
       ROW_NUMBER() OVER (PARTITION BY user_id, session_count ORDER BY user_id, active_date) AS row_count
FROM users_events4)

SELECT gender,
       AVG(avg_events_user) AS avg_num_events,
       AVG(avg_minutes_user) AS avg_session_length
FROM
        (SELECT user_id,
                gender,
                AVG(user_events_per_session) AS avg_events_user,
                AVG(user_minutes_per_session) AS avg_minutes_user
        FROM
                (SELECT user_id,
                        gender,
                        user_events_per_session,
                        user_minutes_per_session
                FROM users_session_data
                WHERE row_count = 1) A --this inner query just selects everything from user_session_data where row count is 1 so we don't get multiple values from the partition
        GROUP BY user_id, gender
        ORDER BY user_id) B --this inner query calculates the averages for each user. The final query will then calculate the global average
GROUP BY gender
ORDER BY gender;

--gender  avg_num_events  avg_session_length
--F	      42.03	          5.13
--M	      33.76	          3.04

--can now drop the tables created as no longer needed
DROP TABLE users_events;
DROP TABLE users_events2;
DROP TABLE users_events3;
DROP TABLE users_events4;
