<?php
 $group_name = "group1";
 $remote_filename = "M00/28/E3/U6Q-CkrMFUgAAAAAAAAIEBucRWc5452.h";
 $file_id = $group_name . FDFS_FILE_ID_SEPERATOR . $remote_filename;

 $tracker = fastdfs_tracker_get_connection();
 var_dump($tracker);

 $server = fastdfs_connect_server($tracker['ip_addr'], $tracker['port']); 
 var_dump($server);
 var_dump(fastdfs_disconnect_server($server));

 //var_dump(fastdfs_tracker_list_groups());
 //var_dump(fastdfs_tracker_query_storage_store());
 //var_dump(fastdfs_tracker_query_storage_update($group_name, $remote_filename));
 //var_dump(fastdfs_tracker_query_storage_fetch($group_name, $remote_filename));
 //var_dump(fastdfs_tracker_query_storage_list($group_name, $remote_filename));

 var_dump(fastdfs_tracker_query_storage_update1($file_id));
 var_dump(fastdfs_tracker_query_storage_fetch1($file_id));
 var_dump(fastdfs_tracker_query_storage_list1($file_id));
 var_dump(fastdfs_storage_upload_by_filename("/usr/include/stdio.h", null));
 var_dump(fastdfs_storage_upload_by_filename1("/usr/include/stdio.h", null, array('width'=>1024, 'height'=>800, 'font'=>'Aris')));
 var_dump(fastdfs_storage_upload_by_filebuff("this is a test.", "txt"));
 var_dump(fastdfs_storage_upload_by_filebuff1("this\000is\000a\000test.", "bin"));

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
 //var_dump($fdfs->tracker_query_storage_list($group_name, $remote_filename));

 var_dump($fdfs->tracker_query_storage_update1($file_id));
 var_dump($fdfs->tracker_query_storage_fetch1($file_id));
 var_dump($fdfs->tracker_query_storage_list1($file_id));
 var_dump($fdfs->storage_upload_by_filename("/usr/include/stdio.h"));
 var_dump($fdfs->storage_upload_by_filename1("/usr/include/stdio.h", "c", array('width'=>1024, 'height'=>800, 'font'=>'Aris')));
 var_dump($fdfs->storage_upload_by_filebuff("this is a test.", "txt"));
 var_dump($fdfs->storage_upload_by_filebuff1("this\000is\001a\002test.", "bin"));
?>
