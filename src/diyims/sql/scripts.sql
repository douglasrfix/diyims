
-- name: create_schema#
CREATE TABLE "header_table" (
	"version" TEXT,
	"object_CID"	TEXT,
	"object_type"	TEXT,
	"insert_DTS"	TEXT,
	"prior_header_CID"	TEXT,
	'header_CID' TEXT
);

CREATE TABLE "peer_table" (
	"peer_ID"	TEXT,
	"IPNS_name"	TEXT,
	"signature" TEXT,
	"signature_valid" TEXT,
	"peer_type" TEXT,
	"origin_update_DTS"	TEXT,
	"local_update_DTS" TEXT,
	"execution_platform"	TEXT,
	"python_version"	TEXT,
	"IPFS_agent"	TEXT,
	"processing_status" TEXT,
	"agent" TEXT,
	"version"	TEXT,
	PRIMARY KEY("peer_ID")
);

CREATE TABLE "want_list_table" (
	"peer_ID"	TEXT,
	"object_CID" TEXT,
	"insert_DTS"	TEXT,
	"last_update_DTS" TEXT,
	"insert_update_delta" INTEGER,
	"source_peer_type"	TEXT,
	PRIMARY KEY("peer_ID", "object_CID")
);

CREATE TABLE "network_table" (
	"network_name"	TEXT
);

CREATE TABLE "log" (
	"DTS"	TEXT,
	"process"	TEXT,
	"pid"	INTEGER,
	"peer_type"	TEXT,
	"msg"	TEXT
);


CREATE TABLE "clean_up" (
	"DTS"	TEXT,
	"want_item_file"	TEXT,
	"beacon_CID"	TEXT,
	PRIMARY KEY("DTS")
);

-- name: set_pragma#
PRAGMA journal_mode = WAL
;

-- name: select_clean_up_rows_by_date
SELECT
	DTS,
	want_item_file,
	beacon_CID

FROM
   clean_up

where DTS < :DTS

-- name: insert_clean_up_row!
insert into clean_up (DTS, want_item_file, beacon_CID)
values (:DTS, :want_item_file, :beacon_CID);

-- name: delete_clean_up_row_by_date!
DELETE

FROM
   clean_up

where DTS = :DTS


-- name: delete_log_rows_by_date!
DELETE

FROM

	log

where DTS < :DTS


-- name: delete_want_list_table_rows_by_date!
DELETE

FROM

	want_list_table

where last_update_DTS < :DTS1 or (insert_DTS < :DTS2 and last_update_DTS = "null")

-- name: insert_log_row!
insert into log (DTS, process, pid, peer_type, msg)
values (:DTS, :process, :pid, :peer_type, :msg);

-- name: insert_peer_row!
insert into peer_table (peer_ID, IPNS_name, signature, signature_valid, peer_type, origin_update_DTS, local_update_DTS, execution_platform, python_version,
		IPFS_agent, processing_status, agent, version)
values (:peer_ID, :IPNS_name, :signature, :signature_valid, :peer_type, :origin_update_DTS, :local_update_DTS,
		:execution_platform, :python_version, :IPFS_agent, :processing_status,
		:agent, :version);

-- name: update_peer_row_by_key!
update peer_table set (peer_ID = :peer_ID, IPNS_name = :IPNS_name, signature = :signature,
	signature_valid = :signature_valid, peer_type = :peer_type, origin_update_DTS = :origin_update_DTS,
	local_update_DTS = :local_update_DTS, execution_platform = :execution_platform, python_version = :python_version,
		IPFS_agent = :IPFS_agent, processing_status = :processing_status, agent = :agent, version = :version)

where peer_ID = :peer_ID

-- name: update_peer_table_peer_type_status!
update peer_table set peer_type = :peer_type, processing_status = :processing_status,
local_update_DTS = :local_update_DTS
where peer_ID = :peer_ID

-- name: update_peer_table_metrics!
update peer_table set origin_update_DTS = :origin_update_DTS,
execution_platform = :execution_platform, python_version = :python_version,
IPFS_agent = :IPFS_agent, agent = :agent
where peer_type = "LP"

-- name: update_peer_table_status_WLR!
update peer_table set processing_status = :processing_status, local_update_DTS = :local_update_DTS
where peer_ID = :peer_ID and  processing_status = "WLX"

-- name: update_peer_table_status_WLP!
update peer_table set processing_status = :processing_status, local_update_DTS = :local_update_DTS
where peer_ID = :peer_ID and  processing_status = "WLR"


-- name: update_peer_table_status_WLX!
update peer_table set processing_status = :processing_status, local_update_DTS = :local_update_DTS
where peer_ID = :peer_ID and  processing_status = "WLP"

-- name: update_peer_table_status_WLZ!
update peer_table set processing_status = :processing_status, local_update_DTS = :local_update_DTS
where peer_ID = :peer_ID and  processing_status = "WLX"


-- name: update_peer_table_IPNS_name_status_NPC!
update peer_table set IPNS_name = :IPNS_name, processing_status = :processing_status, local_update_DTS = :local_update_DTS
where peer_ID = :peer_ID and (processing_status = "WLR" or processing_status = "WLP" or
	processing_status = "WLX" or processing_status = "WLZ")


-- name: reset_peer_table_status#
update peer_table set processing_status = "WLR"
where processing_status  = "WLX" or processing_status = "WLP"

-- name: select_peers_by_peer_type_status^
SELECT
	peer_ID,
	IPNS_name,
	peer_type,
   	origin_update_DTS,
	local_update_DTS,
   	execution_platform,
	python_version,
   	IPFS_agent,
	processing_status,
	agent,
 	version

FROM
   peer_table

where peer_type = :peer_type and (processing_status = "WLR")
ORDER BY

	local_update_DTS ASC

-- name: select_peer_table_entry_by_key^
SELECT
	peer_ID,
	IPNS_name,
	peer_type,
   	origin_update_DTS,
	local_update_DTS,
   	execution_platform,
	python_version,
   	IPFS_agent,
	processing_status,
	agent,
 	version

FROM
   peer_table

where peer_ID = :peer_ID

-- name: select_peer_table_local_peer_entry^
SELECT
	peer_ID,
	IPNS_name,
	signature,
	signature_valid,
	peer_type,
   	origin_update_DTS,
	local_update_DTS,
   	execution_platform,
	python_version,
   	IPFS_agent,
	processing_status,
	agent,
 	version

FROM
   peer_table

where peer_type = "LP"

-- name: insert_header_row!
insert into header_table (version, object_CID, object_type, insert_DTS,
	 prior_header_CID, header_CID)
values (:version, :object_CID, :object_type, :insert_DTS, :prior_header_CID, :header_CID);

-- name: insert_want_list_row!
insert into want_list_table (peer_ID, object_CID, insert_DTS, last_update_DTS, insert_update_delta, source_peer_type)
values (:peer_ID, :object_CID, :insert_DTS, :last_update_DTS,
 :insert_update_delta, :source_peer_type);

-- name: update_last_update_DTS!
update want_list_table set last_update_DTS = :last_update_DTS,
 insert_update_delta = :insert_update_delta
where peer_ID = :peer_ID and object_CID = :object_CID



-- name: insert_network_row!
insert into network_table (network_name)
values (:network_name);

-- name: select_last_header^
SELECT
 	version,
   	object_CID,
   	object_type,
   	insert_DTS,
   	prior_header_CID,
   	header_CID

FROM
   header_table

ORDER BY

	insert_DTS DESC
;

-- name: select_first_peer_row_entry_pointer^
SELECT
 	version,
   	object_CID,
   	object_type,
   	insert_DTS,
   	prior_header_CID,
   	header_CID

FROM
   header_table

WHERE object_type = "peer_row_entry"

ORDER BY

	insert_DTS ASC
;

-- name: select_all_headers
SELECT
 	version,
   	object_CID,
   	object_type,
   	insert_DTS,
   	prior_header_CID,
   	header_CID

FROM
   header_table

;

-- name: select_network_name^
SELECT
   	network_name

FROM
   network_table

;

-- name: select_want_list_entry_by_key^
select peer_ID, object_CID, insert_DTS, last_update_DTS, insert_update_delta, source_peer_type
from want_list_table
where peer_ID = :peer_ID and object_CID = :object_CID
;

-- name: select_filter_want_list_by_start_stop
select peer_ID, object_CID, insert_DTS, last_update_DTS, insert_update_delta, source_peer_type
from want_list_table
where last_update_DTS >= :query_start_dts and last_update_DTS <= :query_stop_dts
	and (insert_update_delta < 294 and insert_update_delta > 285)
;
