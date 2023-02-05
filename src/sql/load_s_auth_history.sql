--load s_auth_history
INSERT INTO BADASOVANTYANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
SELECT luga.hk_l_user_group_activity,
       gl.user_id_from,
	   gl.event,
	   gl.datetime AS event_dt,
	   now() as load_dt,
	   's3' as load_src
  FROM BADASOVANTYANDEXRU__STAGING.group_log AS gl
 LEFT JOIN BADASOVANTYANDEXRU__DWH.h_groups AS hg ON gl.group_id = hg.group_id
 LEFT JOIN BADASOVANTYANDEXRU__DWH.h_users AS hu ON gl.user_id = hu.user_id
 LEFT JOIN BADASOVANTYANDEXRU__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
; 


