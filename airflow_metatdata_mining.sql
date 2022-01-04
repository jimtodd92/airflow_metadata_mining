-- To get Critical user activities like paused and dagrun_clear in past 24 hours.
SELECT event
	,DATE (dttm)
	,count(*)
FROM PUBLIC.log
WHERE DATE (dttm) BETWEEN (
				SELECT max(DATE (dttm)) - 1
				FROM PUBLIC.log
				)
		AND (
				SELECT max(DATE (dttm))
				FROM PUBLIC.log
				)
	AND event IN (
		'paused'
		,'dagrun_clear'
		)
GROUP BY event
	,DATE (dttm)
ORDER BY DATE (dttm) DESC;


-- To get 10 long ran tasks in a day.
SELECT dag_id
	,task_id
	,execution_date
	,round(cast(duration / (60 * 60) AS NUMERIC), 2) AS duration_in_hour
FROM PUBLIC.task_instance
WHERE DATE (execution_date) = '2022-01-03'
	AND STATE IN ('success')
	AND duration IS NOT NULL
GROUP BY task_id
	,dag_id
	,execution_date
ORDER BY duration DESC limit 10;


-- To understand how occupied the airflow scheduler is, and decide a maintenance window.
SELECT x.start_time_window
	,count(*)
FROM (
	SELECT start_date
		,CASE
			WHEN extract(hour FROM start_date) BETWEEN 0
					AND 5
				THEN '0-5'
			WHEN extract(hour FROM start_date) BETWEEN 6
					AND 11
				THEN '6-11'
			WHEN extract(hour FROM start_date) BETWEEN 12
					AND 17
				THEN '12-17'
			WHEN extract(hour FROM start_date) BETWEEN 18
					AND 23
				THEN '18-23'
			END AS start_time_window
	FROM PUBLIC.task_instance
	WHERE execution_date BETWEEN '2022-01-01'
			AND '2022-01-02'
		AND start_date IS NOT NULL
	) AS x
GROUP BY x.start_time_window
ORDER BY cast(split_part(x.start_time_window, '-', 1) AS INT)