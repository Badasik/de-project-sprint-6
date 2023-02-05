DROP TABLE IF EXISTS BADASOVANTYANDEXRU__STAGING.group_log;

CREATE TABLE BADASOVANTYANDEXRU__STAGING.group_log (
	group_id						INT PRIMARY KEY,
	user_id							INT NOT NULL,
	user_id_from					INT,
	event							VARCHAR(6),
	"datetime"						TIMESTAMP(6))
ORDER BY group_id SEGMENTED BY HASH(group_id) ALL nodes PARTITION BY datetime::DATE
GROUP BY calendar_hierarchy_day(datetime::DATE, 3, 2);
   
   

SELECT *
FROM BADASOVANTYANDEXRU__STAGING.group_log

