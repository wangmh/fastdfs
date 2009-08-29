/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include "client_global.h"

int g_tracker_server_count = 0;
int g_tracker_server_index = 0;
int g_tracker_server_http_port = 80;
TrackerServerInfo *g_tracker_servers = NULL;

bool g_anti_steal_token = false;
BufferInfo g_anti_steal_secret_key = {0};
