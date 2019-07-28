--query to get base stats for players who bought a specific feature
WITH
--to get all transaction with event name
table_1 AS ( 
	SELECT DISTINCT
		table_2.col_a,
		table_2.col_b,
		col_c,
		col_d,
		col_e,
		col_f,
		col_e,
		col_g,
		col_h,
		col_i,
		col_j,
		col_k,
		col_l,
		CASE WHEN (MapJSONExtractor(LOWER(col_m)))['string'] = 1 THEN 'with_feature' ELSE 'no_feature' END AS feature_occured
	FROM table_2
		JOIN table_3 ON table_3.col_b = table_2.col_b AND table_3.col_a = table_2.col_a
		WHERE col_e = 1 AND col_g = 'N'
),
-- a specific feature basic stats for a game
game_feature AS(
	SELECT
		COUNT(DISTINCT HASH(col_c, col_a)) AS feature_Users,
		COUNT(DISTINCT HASH(col_b, col_a)) AS feature_num_bets,
		SUM(col_j) AS feature_bet,
		SUM(col_j) - SUM(col_k) AS feature_GW,
		SUM(col_j) / COUNT(DISTINCT HASH(col_b, col_a)) AS feature_avg_bet
	FROM table_1
	WHERE feature_occured = 'with_feature'
),
-- all transacions basic stats for a game
game_whole AS (
	SELECT 
		COUNT(DISTINCT HASH(col_c, col_a)) AS ALL_Users,
		SUM(number_of_bets) AS ALL_num_bets,
		SUM(bet) AS ALL_bet,
		SUM(GW) AS ALL_GW,
		SUM(bet) / SUM(number_of_bets) AS ALL_avg_bet		
	FROM table_4
	WHERE col_h = 1 )
-- results
SELECT 
	feature_Users,
	(feature_Users / ALL_Users)*100 AS proc_feature_Users_vs_All,
	feature_num_bets,
	(feature_num_bets / ALL_num_bets)*100 AS proc_feature_num_bets,
	feature_bet,
	(feature_bet / ALL_bet)*100 AS proc_feature_bet,
	feature_GW,
	(feature_GW / ALL_GW)*100 proc_feature_GW,
	feature_avg_bet,
	ALL_avg_bet	
FROM game_feature 
	JOIN game_whole ON 1;
-----------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------
-- guery basic stats for a specific feature vs all results per col_d
--to get all transaction with event name
WITH
table_1 AS ( 
	SELECT DISTINCT
		table_2.col_a,
		table_2.col_b,
		col_c,
		col_d,
		col_e,
		col_f,
		col_e,
		col_g,
		col_h,
		col_i,
		col_j,
		col_k,
		col_l,
		CASE WHEN (MapJSONExtractor(LOWER(col_m)))['string'] = 1 THEN 'with_feature' ELSE 'no_feature' END AS feature_occured
	FROM table_2
		JOIN table_3 ON table_3.col_b =  table_2.col_b AND table_3.col_a =  table_2.col_a
		WHERE col_e = 1 AND col_g = 'N'
),
-- a specific feature basic stats for a game
game_feature AS(
	SELECT
		col_d,
		COUNT(DISTINCT HASH(col_c, col_a)) AS feature_Users,
		COUNT(DISTINCT HASH(col_b, col_a)) AS feature_num_bets,
		SUM(col_j) AS feature_bet,
		SUM(col_j) - SUM(col_k) AS feature_GW,
		SUM(col_j) / COUNT(DISTINCT HASH(col_b, col_a)) AS feature_avg_bet
	FROM table_1
	WHERE feature_occured = 'with_feature'
	GROUP BY 1
),
-- All bets basic stats for a game
game_whole AS (  
	SELECT
		col_d, 
		COUNT(DISTINCT HASH(col_c, col_a)) AS ALL_Users,
		SUM(number_of_bets) AS ALL_num_bets,
		SUM(bet) AS ALL_bet,
		SUM(GW) AS ALL_GW,
		SUM(bet) / SUM(number_of_bets) AS ALL_avg_bet		
	FROM table_4
	WHERE col_h = 1
	GROUP BY 1)
-- results
SELECT 
	game_feature.col_d, 
	feature_Users,
	(feature_Users / ALL_Users)*100 AS proc_feature_Users_vs_All,
	feature_num_bets,
	(feature_num_bets / ALL_num_bets)*100 AS proc_feature_num_bets,
	feature_bet,
	(feature_bet / ALL_bet)*100 AS proc_feature_bet,
	feature_GW,
	(feature_GW / ALL_GW)*100 proc_feature_GW,
	feature_avg_bet,
	ALL_avg_bet	
FROM game_feature 
	JOIN game_whole ON game_whole.col_d =  game_feature.col_d 
ORDER BY 2 DESC;
-------------------------------------------------------------------------------------------------------


-------------------------------------------------------------------------------------------------------
--query to calculate how many users did 'x' feature
WITH table_1 AS ( --to get all transaction with event name
	SELECT DISTINCT
		table_2.col_a,
		table_2.col_b,
		col_c,
		col_d,
		col_e,
		col_f,
		col_e,
		col_g,
		col_h,
		col_i,
		col_j,
		col_k,
		col_l,
		CASE WHEN (MapJSONExtractor(LOWER(col_m)))['string'] = 1 THEN 'with_feature' ELSE 'no_feature' END AS feature_occured
	FROM table_2
		JOIN table_3 ON table_3.col_b =  table_2.col_b AND table_3.col_a =  table_2.col_a 
		WHERE col_e = 1 AND col_g = 'N'
),
--query to calculate how many users did 'x' feature
transaction_feature AS (
	SELECT
		col_a,
		col_b,
		col_c
	FROM table_1
	WHERE feature_occured = 'with_feature'
),
-- to get number of feature per user
feature_per_user AS (
	SELECT
		HASH(col_c, col_a) AS unique_col_c,
		COUNT(DISTINCT HASH(col_b, col_a)) AS num_feature_per_user
	FROM transaction_feature
	GROUP BY 1
),
-- to get how many user did 'x' feature
user_per_feature AS (
	SELECT 
		num_feature_per_user,
		COUNT(unique_col_c) AS num_users
	FROM feature_per_user 
	GROUP BY 1)
--to calculate % of total
SELECT
	num_feature_per_user,
	num_users,
	ROUND((num_users / (SUM(num_users) OVER())) * 100, 1) AS proc_of_total
FROM user_per_feature
order by 1;

-----------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------
--to get AVG Turnover (in whole game) per player who bought feature 
WITH table_1 AS ( --to get all transaction with event name
	SELECT DISTINCT
		col_a,
		col_c,
		col_e,
		col_g,
		CASE WHEN (MapJSONExtractor(LOWER(col_m)))['string'] = 1 THEN 'with_feature' ELSE 'no_feature' END AS feature_occured
	FROM table_2),
--to get transaction only for feature
transaction_feature AS (
	SELECT 
		*
	FROM table_1
	WHERE feature_occured = 'with_feature' AND col_e = 1 AND col_g = 'N'
),
-----
FGF AS (
	SELECT DISTINCT
		table_4.col_c,
		table_4.col_a,
		SUM(table_4.bet) AS FGF_bet
	FROM transaction_feature
	JOIN table_4 ON table_4.col_c = transaction_feature.col_c AND table_4.col_a = transaction_feature.col_a AND col_h = 1
	GROUP BY 1,2)
--results
SELECT
	SUM(FGF_bet) / COUNT(DISTINCT hash(col_c, col_a)) AS avg_turnover
FROM FGF;
-----------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------
--to get AVG Turnover (in whole game) per player who DID NOT buy feature 
WITH table_1 AS ( --to get all transaction with event name
	SELECT DISTINCT
		col_a,
		col_c,
		col_e,
		col_g,
		CASE WHEN (MapJSONExtractor(LOWER(col_m)))['string'] = 1 THEN 'with_feature' ELSE 'no_feature' END AS feature_occured
	FROM table_2),
--to get transaction only for feature
transaction_feature AS (
	SELECT 
		*
	FROM table_1
	WHERE feature_occured = 'no_feature' AND col_e = 1 AND col_g = 'N'
),
-----
FGF AS (
	SELECT DISTINCT
		table_4.col_c,
		table_4.col_a,
		SUM(table_4.bet) AS FGF_bet
	FROM transaction_feature
	JOIN table_4 ON table_4.col_c = transaction_feature.col_c AND table_4.col_a = transaction_feature.col_a AND col_h = 1
	GROUP BY 1,2)
--results
SELECT
	SUM(FGF_bet) / COUNT(DISTINCT hash(col_c, col_a)) AS avg_turnover
FROM FGF;
-----------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------



