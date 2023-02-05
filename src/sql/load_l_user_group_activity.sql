--Load l_user_group_activity
INSERT INTO BADASOVANTYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT HASH(hu.hk_user_id, hk_group_id),
	   hu.hk_user_id,
	   hg.hk_group_id,
	   now() AS load_dt,
	   's3' AS load_src 
  FROM BADASOVANTYANDEXRU__STAGING.group_log AS g
  LEFT JOIN BADASOVANTYANDEXRU__DWH.h_users AS hu ON hu.user_id = g.user_id 
  LEFT JOIN BADASOVANTYANDEXRU__DWH.h_groups AS hg ON hg.group_id  = g.group_id 
  WHERE HASH(hu.hk_user_id, hk_group_id) NOT IN (SELECT hk_l_user_group_activity
   												   FROM BADASOVANTYANDEXRU__DWH.l_user_group_activity
  													)