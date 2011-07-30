<?php
 define('FILE_BUFF', "this is a test");

 echo 'FastDFS Client Version: ' . fastdfs_client_version() . "\n";

 $callback_arg = array ( 'buff' => FILE_BUFF);
 $callback_array = array(
		'callback' => 'my_upload_file_callback', 
		'file_size' => strlen(FILE_BUFF), 
		'args' => $callback_arg);

 $file_info = fastdfs_storage_upload_by_callback($callback_array);
 if ($file_info)
 {
	$group_name = $file_info['group_name'];
	$remote_filename = $file_info['filename'];

	var_dump($file_info);
	var_dump(fastdfs_get_file_info($group_name, $remote_filename));
 }
 else
 {
	echo "upload file fail, errno: " . fastdfs_get_last_error_no() . ", error info: " . fastdfs_get_last_error_info() . "\n";
 }

 $file_id = fastdfs_storage_upload_by_callback1($callback_array, 'txt');
 if ($file_id)
 {
	var_dump($file_id);
 }
 else
 {
	echo "upload file fail, errno: " . fastdfs_get_last_error_no() . ", error info: " . fastdfs_get_last_error_info() . "\n";
 }

 $fdfs = new FastDFS();
 $file_info = $fdfs->storage_upload_by_callback($callback_array, 'txt');
 if ($file_info)
 {
	$group_name = $file_info['group_name'];
	$remote_filename = $file_info['filename'];

	var_dump($file_info);
	var_dump($fdfs->get_file_info($group_name, $remote_filename));
 }
 else
 {
	echo "upload file fail, errno: " . $fdfs->get_last_error_no() . ", error info: " . $fdfs->get_last_error_info() . "\n";
 }

 $file_id = $fdfs->storage_upload_by_callback1($callback_array, 'txt');
 if ($file_id)
 {
	var_dump($file_id);
 }
 else
 {
	echo "upload file fail, errno: " . $fdfs->get_last_error_no() . ", error info: " . $fdfs->get_last_error_info() . "\n";
 }

 function my_upload_file_callback($sock, $args)
 {
	//var_dump($args);

	return fastdfs_send_data($sock, $args['buff']);
 }
?>
