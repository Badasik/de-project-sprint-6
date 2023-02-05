WITH user_group_messages AS (
		SELECT lgd.hk_group_id AS hk_group_id,
			   COUNT(DISTINCT lum.hk_user_id) AS cnt_user_in_group_with_messages
		  FROM BADASOVANTYANDEXRU__DWH.l_user_message lum 
		  LEFT JOIN BADASOVANTYANDEXRU__DWH.l_groups_dialogs lgd ON lgd.hk_message_id = lum.hk_message_id
		 GROUP BY lgd.hk_group_id
	 ), 
 	 user_group_log AS (
		SELECT luga.hk_group_id,
	           COUNT(DISTINCT luga.hk_user_id) cnt_added_users
		  FROM BADASOVANTYANDEXRU__DWH.l_user_group_activity luga 
		 INNER JOIN BADASOVANTYANDEXRU__DWH.s_auth_history sah ON ISNULL(sah.hk_l_user_group_activity, -1) = ISNULL(luga.hk_l_user_group_activity, -1)
	       AND sah.event = 'add'
	     GROUP BY luga.hk_group_id
	     ),
	 oldest_groups AS (
	 	SELECT hk_group_id
	 	  FROM BADASOVANTYANDEXRU__DWH.h_groups hg 
	 	 ORDER BY registration_dt
	 	 LIMIT 10)
SELECT og.hk_group_id,
	   ugl.cnt_added_users,
	   ugm.cnt_user_in_group_with_messages,
	   ugm.cnt_user_in_group_with_messages / ugl.cnt_added_users AS group_conversion
  FROM oldest_groups AS og
 INNER JOIN user_group_messages ugm ON ugm.hk_group_id = og.hk_group_id
 INNER JOIN user_group_log ugl ON ugl.hk_group_id = og.hk_group_id
 ORDER BY ugm.cnt_user_in_group_with_messages / ugl.cnt_added_users DESC
