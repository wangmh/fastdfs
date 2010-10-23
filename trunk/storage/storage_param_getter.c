/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/statvfs.h>
#include <sys/param.h>
#include "fdfs_define.h"
#include "logger.h"
#include "fdfs_global.h"
#include "tracker_types.h"
#include "tracker_proto.h"
#include "storage_global.h"
#include "storage_param_getter.h"

int storage_get_params_from_tracker()
{
	IniContext iniContext;
	int result;

	if ((result=fdfs_get_ini_context_from_tracker(&g_tracker_group, \
		&iniContext, &g_continue_flag, \
		g_client_bind_addr, g_bind_addr)) != 0)
	{
		return result;
	}

	g_storage_ip_changed_auto_adjust = iniGetBoolValue(NULL, \
			"storage_ip_changed_auto_adjust", \
			&iniContext, false);

	iniFreeContext(&iniContext);

	logInfo("file: "__FILE__", line: %d, " \
		"storage_ip_changed_auto_adjust=%d", __LINE__, \
		g_storage_ip_changed_auto_adjust);

	return 0;
}

