--Query to find VIPs players, assuming in where condition statment at the end of query
--Assumption - player has x days to achieve the condition
CREATE TABLE VIP_list AS (
SELECT col_1, 
	col_2,
	first_activity_as_VIP,	
	last_activity_as_VIP,
	first_activity_at_all,
	last_activity_at_all,
	active_days_as_NO_VIP,
	active_days_as_VIP,
	col_4DIFF(day, last_activity_at_all, now()) AS inactive_days  
	FROM (SELECT col_1, 
				col_3, 
				col_2, 
				first_activity_as_VIP,				
				last_activity_as_VIP,
				first_activity_at_all,				
				last_activity_at_all,
				active_days_as_VIP,
				COUNT(DISTINCT CASE WHEN col_4 BETWEEN last_activity_as_VIP AND last_activity_at_all THEN col_4 ELSE NULL END) AS active_days_as_NO_VIP
			FROM(SELECT DISTINCT col_1, 
						col_3, 
						col_2, 
						col_4, 
						--condition statmnet for VIP
						MAX(CASE WHEN sum_bet_x_days >= 1 AND avg_bet >= 1 AND sum_gw_x_days >= 1 THEN col_4 ELSE NULL END) OVER(PARTITION BY col_1, col_3, col_2) AS last_activity_as_VIP, 
						MIN(CASE WHEN sum_bet_x_days >= 1 AND avg_bet >= 1 AND sum_gw_x_days >= 1 THEN col_4 ELSE NULL END) OVER(PARTITION BY col_1, col_3, col_2) AS first_activity_as_VIP,
						COUNT( CASE WHEN sum_bet_x_days >= 1 AND avg_bet >= 1 AND sum_gw_x_days >= 1 THEN col_4 ELSE NULL END) OVER(PARTITION BY col_1, col_3, col_2) AS active_days_as_VIP,
						last_activity_at_all,
						first_activity_at_all
						FROM (
							--distinct because few hours in one day give few the same records
							--windowns frame x days
							SELECT DISTINCT col_1, 
									col_3, 
									col_2,
									col_4, 
									--max col_4 = last activity
									MAX(col_4) OVER(PARTITION BY col_1, col_3, col_2) AS last_activity_at_all,
									MIN(col_4) OVER(PARTITION BY col_1, col_3, col_2) AS first_activity_at_all,
									--OVER()...to calculate running SUM for period in INTERVAL
									--this calculation take gaps in period also
									SUM(gw) OVER(PARTITION BY col_1, col_3, col_2 ORDER BY col_4
												RANGE BETWEEN INTERVAL 'x days'
												PRECEDING AND CURRENT ROW) AS sum_gw_x_days,
									SUM(egw) OVER(PARTITION BY col_1, col_3, col_2 ORDER BY col_4
												RANGE BETWEEN INTERVAL 'x days'
												PRECEDING AND CURRENT ROW) AS sum_egw_x_days,
									SUM(number_of_bets) OVER(PARTITION BY col_1, col_3, col_2 ORDER BY col_4
												RANGE BETWEEN INTERVAL 'x days'
												PRECEDING AND CURRENT ROW) AS sum_num_bets_x_days,
									SUM(bet) OVER(PARTITION BY col_1, col_3, col_2 ORDER BY col_4
												RANGE BETWEEN INTERVAL 'x days'
												PRECEDING AND CURRENT ROW) AS sum_bet_x_days,				
									SUM(bet) OVER(PARTITION BY col_1, col_3, col_2 ORDER BY col_4 
												RANGE BETWEEN INTERVAL 'x days' PRECEDING AND CURRENT ROW) / 
									SUM(number_of_bets) OVER(PARTITION BY col_1, col_3, col_2 ORDER BY col_4 
									RANGE BETWEEN INTERVAL 'x days' PRECEDING AND CURRENT ROW) avg_bet 
							FROM table_1
							WHERE col_4 >= '1000-01-01' and col_2 is not null
							ORDER BY col_4) a
				) b
		GROUP BY col_1, col_3, col_2, last_activity_as_VIP, last_activity_at_all,first_activity_as_VIP, first_activity_at_all, active_days_as_VIP
		--to select only users with condintion statment
		HAVING last_activity_as_VIP IS NOT NULL) c
)