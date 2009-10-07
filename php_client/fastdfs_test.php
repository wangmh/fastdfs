<?php
 $group_name = "group1";
 $remote_filename = "M00/28/E3/U6Q-CkrMFUgAAAAAAAAIEBucRWc5452.h";

 $tracker = fastdfs_tracker_get_connection();
 var_dump($tracker);

 $server = fastdfs_connect_server($tracker['ip_addr'], $tracker['port']); 
 var_dump($server);
 var_dump(fastdfs_disconnect_server($server));

 //var_dump(fastdfs_tracker_list_groups());
 //var_dump(fastdfs_tracker_query_storage_store());
 //var_dump(fastdfs_tracker_query_storage_update($group_name, $remote_filename));
 //var_dump(fastdfs_tracker_query_storage_fetch($group_name, $remote_filename));
 var_dump(fastdfs_tracker_query_storage_list($group_name, $remote_filename));

 $fdfs = new FastDFS();
 $tracker = $fdfs->tracker_get_connection();
 var_dump($tracker);

 $server = $fdfs->connect_server($tracker['ip_addr'], $tracker['port']);
 var_dump($server);
 var_dump($fdfs->disconnect_server($server));

 //var_dump($fdfs->tracker_list_groups());
 //var_dump($fdfs->tracker_query_storage_store());
 //var_dump($fdfs->tracker_query_storage_update($group_name, $remote_filename));
 //var_dump($fdfs->tracker_query_storage_fetch($group_name, $remote_filename));
 var_dump($fdfs->tracker_query_storage_list($group_name, $remote_filename));

?>
