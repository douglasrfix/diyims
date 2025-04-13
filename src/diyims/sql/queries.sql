select peer_ID, source_peer_type, insert_DTS, last_update_DTS, insert_update_delta
 from want_list_table
 where  (insert_update_delta < 295 and insert_update_delta > 285)
;


select peer_ID, source_peer_type, insert_DTS, last_update_DTS, insert_update_delta, 'S'
 from want_list_table
 where (source_peer_type = "NP")
and (insert_update_delta < 60 and insert_update_delta > 45)
 order by peer_ID, insert_DTS ASC
;

select peer_ID, source_peer_type, insert_DTS, last_update_DTS, insert_update_delta, 'S'
 from want_list_table
 where (source_peer_type = "NP")
 order by peer_ID, insert_DTS ASC
;



select peer_ID, source_peer_type, last_update_DTS, insert_update_delta
 from want_list_table
 where (insert_update_delta <= 60 and insert_update_delta >= 45)
 or (insert_update_delta <= 30 and insert_update_delta >= 14)
 order by peer_ID, insert_DTS ASC
;

select peer_ID, source_peer_type, insert_DTS, last_update_DTS, insert_update_delta

 from want_list_table
 where insert_update_delta >= 285 and insert_update_delta <= 295

 order by insert_DTS ASC
;

select object_CID, peer_ID, insert_DTS, last_update_DTS
 from want_list_table
 where insert_DTS != last_update_DTS
 order by insert_DTS ASC
;

select * from peer_table where peer_type = "NP";

update peer_table set processing_status = "null"
where peer_type = "BP";


delete from want_list_table
;

delete from log
;

delete from peer_table
where peer_type = "BP" or  peer_type= "SP"
;
