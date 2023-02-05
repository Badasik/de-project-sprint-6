---l_user_group_activity
DROP TABLE IF EXISTS BADASOVANTYANDEXRU__DWH.l_user_group_activity

CREATE TABLE BADASOVANTYANDEXRU__DWH.l_user_group_activity(
	hk_l_user_group_activity						INT PRIMARY KEY,
	hk_user_id										INT NOT NULL CONSTRAINT fk_l_user_group_activity_h_users REFERENCES BADASOVANTYANDEXRU__DWH.h_users(hk_user_id),
	hk_group_id										INT NOT NULL CONSTRAINT fk_l_user_group_activity_h_groups REFERENCES BADASOVANTYANDEXRU__DWH.h_groups(hk_group_id),
	load_dt											DATETIME,
	load_src										VARCHAR(20));

ORDER BY
    load_dt SEGMENTED BY hk_user_id ALL nodes PARTITION BY load_dt :: DATE
GROUP BY
    calendar_hierarchy_day(load_dt :: DATE, 3, 2);
	
---s_auth_history
CREATE TABLE BADASOVANTYANDEXRU__DWH.s_auth_history(
	hk_l_user_group_activity						INT NOT NULL CONSTRAINT fk_l_user_group_activity REFERENCES BADASOVANTYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity),
	user_id_from									INT,
	event											VARCHAR(6),
	event_dt										TIMESTAMP(6),
	load_dt											DATETIME,
	load_src										VARCHAR(20))
ORDER BY event_dt SEGMENTED BY hk_l_user_group_activity ALL nodes
PARTITION BY event_dt::DATE
GROUP BY calendar_hierarchy_day(event_dt :: DATE, 3, 2);



