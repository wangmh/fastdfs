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
 var_dump(fastdfs_storage_upload_by_filename1("/usr/include/stdio.h", null, array('width'=>1024, 'height'=>800, 'font'=>'Aris', 'Homepage' => true, 'price' => 103.75, 'status' => FDFS_STORAGE_STATUS_ACTIVE)));
 $file_info = fastdfs_storage_upload_by_filebuff("this is a test.", "txt");
 if ($file_info)
 {
	var_dump($file_info);
	$file_content = fastdfs_storage_download_file_to_buff($file_info['group_name'], $file_info['filename']);
	echo "file content: " . $file_content . "(" . strlen($file_content) . ")\n";
 	$local_filename = 't1.txt';
	echo 'storage_download_file_to_file result: ' . 
		fastdfs_storage_download_file_to_file($file_info['group_name'], $file_info['filename'], $local_filename) . "\n";

	echo "fastdfs_storage_set_metadata result: " . fastdfs_storage_set_metadata( 
		$file_info['group_name'], $file_info['filename'], 
		array('color'=>'yellow', 'size'=>32), FDFS_STORAGE_SET_METADATA_FLAG_OVERWRITE) . "\n";

	echo "delete file return: " . fastdfs_storage_delete_file($file_info['group_name'], $file_info['filename']) . "\n";
 }

 $file_id = fastdfs_storage_upload_by_filebuff1("this\000is\000a\000test.", "bin");
 if ($file_id)
 {
	$file_content = fastdfs_storage_download_file_to_buff1($file_id);
	echo "file content: " . $file_content . "(" . strlen($file_content) . ")\n";
 	$local_filename = 't2.txt';
	echo 'storage_download_file_to_file1 result: ' . 
		fastdfs_storage_download_file_to_file1($file_id, $local_filename) . "\n";
	echo "fastdfs_storage_set_metadata1 result: " . fastdfs_storage_set_metadata1( 
		$file_id, array('color'=>'yellow', 'size'=>32), FDFS_STORAGE_SET_METADATA_FLAG_MERGE) . "\n";
	echo "delete file $file_id return: " . fastdfs_storage_delete_file1($file_id) . "\n";
 }

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
 $file_info = $fdfs->storage_upload_by_filebuff("", "txt");

 if ($file_info)
 {
	var_dump($file_info);
	$file_content = $fdfs->storage_download_file_to_buff($file_info['group_name'], $file_info['filename']);
	echo "file content: " . $file_content . "(" . strlen($file_content) . ")\n";
 	$local_filename = 't3.txt';
	echo 'storage_download_file_to_file result: ' . 
		$fdfs->storage_download_file_to_file($file_info['group_name'], $file_info['filename'], $local_filename) . "\n";

	echo "storage_set_metadata result: " . $fdfs->storage_set_metadata( 
		$file_info['group_name'], $file_info['filename'], 
		array('color'=>'yellow', 'size'=>32), FDFS_STORAGE_SET_METADATA_FLAG_OVERWRITE) . "\n";

	echo "delete file return: " . $fdfs->storage_delete_file($file_info['group_name'], $file_info['filename']) . "\n";
 }

 $file_id = $fdfs->storage_upload_by_filebuff1("this\000is\001a\002test.", "bin");
 if ($file_id)
 {
	$file_content = $fdfs->storage_download_file_to_buff1($file_id);
	echo "file content: " . $file_content . "(" . strlen($file_content) . ")\n";
 	$local_filename = 't4.txt';
	echo 'storage_download_file_to_file1 result: ' . $fdfs->storage_download_file_to_file1($file_id, $local_filename) . "\n";
	echo "storage_set_metadata1 result: " . $fdfs->storage_set_metadata1( 
		$file_id, array('color'=>'yellow', 'size'=>32), FDFS_STORAGE_SET_METADATA_FLAG_MERGE) . "\n";
        echo "delete file $file_id return: " . $fdfs->storage_delete_file1($file_id) . "\n";
 }
?>
