tmp_src_filename=fdfs_check_bits.c
cat <<EOF > $tmp_src_filename
#include <stdio.h>
int main()
{
	printf("%d\n", sizeof(long));
	return 0;
}
EOF

gcc $tmp_src_filename
bytes=`./a.out`

/bin/rm -f  a.out $tmp_src_filename
if [ "$bytes" -eq 8 ]; then
 OS_BITS=64
else
 OS_BITS=32
fi

cat <<EOF > common/_os_bits.h
#ifndef _OS_BITS_H
#define _OS_BITS_H

#define OS_BITS  $OS_BITS

#endif
EOF

TARGET_PATH=/usr/local/bin

#WITH_HTTPD=1
#WITH_LINUX_SERVICE=1

CFLAGS='-O3 -Wall -D_FILE_OFFSET_BITS=64'
#CFLAGS='-g -Wall -D_FILE_OFFSET_BITS=64 -D__DEBUG__'

LIBS=''
uname=`uname`
if [ "$uname" = "Linux" ]; then
  CFLAGS="$CFLAGS -DOS_LINUX"
elif [ "$uname" = "FreeBSD" ]; then
  CFLAGS="$CFLAGS -DOS_FREEBSD"
elif [ "$uname" = "SunOS" ]; then
  CFLAGS="$CFLAGS -DOS_SUNOS"
  LIBS="$LIBS -lsocket -lnsl -lresolv"
  export CC=gcc
elif [ "$uname" = "AIX" ]; then
  CFLAGS="$CFLAGS -DOS_AIX"
  export CC=gcc
fi

if [ "$WITH_HTTPD" = "1" ]; then
  CFLAGS="$CFLAGS -DWITH_HTTPD"
  LIBS="$LIBS -levent"
  TRACKER_HTTPD_OBJS='tracker_httpd.o ../common/mime_file_parser.o ../common/fdfs_http_shared.o'
  STORAGE_HTTPD_OBJS='storage_httpd.o ../common/mime_file_parser.o ../common/fdfs_http_shared.o'
else
  TRACKER_HTTPD_OBJS=''
  STORAGE_HTTPD_OBJS=''
fi

if [ -f /usr/lib/libpthread.so ] || [ -f /usr/local/lib/libpthread.so ] || [ -f /usr/lib64/libpthread.so ] || [ -f /usr/lib/libpthread.a ] || [ -f /usr/local/lib/libpthread.a ] || [ -f /usr/lib64/libpthread.a ]; then
  LIBS="$LIBS -lpthread"
else
  line=`nm -D /usr/lib/libc_r.so | grep pthread_create | grep -w T`
  if [ -n "$line" ]; then
    LIBS="$LIBS -lc_r"
  fi
fi

cd tracker
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PATH\)#$TARGET_PATH#g" Makefile
perl -pi -e "s#\\\$\(TRACKER_HTTPD_OBJS\)#$TRACKER_HTTPD_OBJS#g" Makefile
make $1 $2

cd ../storage
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PATH\)#$TARGET_PATH#g" Makefile
perl -pi -e "s#\\\$\(STORAGE_HTTPD_OBJS\)#$STORAGE_HTTPD_OBJS#g" Makefile
make $1 $2

cd ../client
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PATH\)#$TARGET_PATH#g" Makefile
make $1 $2

cd test
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PATH\)#$TARGET_PATH#g" Makefile
cd ..

if [ "$1" = "install" ]; then
  cd ..
  cp -f restart.sh $TARGET_PATH
  cp -f stop.sh $TARGET_PATH

  if [ "$uname" = "Linux" ]; then
    if [ "$WITH_LINUX_SERVICE" = "1" ]; then
      mkdir -p /etc/fdfs
      cp -f conf/tracker.conf /etc/fdfs/
      cp -f conf/storage.conf /etc/fdfs/
      cp -f conf/client.conf /etc/fdfs/
      cp -f conf/http.conf /etc/fdfs/
      cp -f conf/mime.types /etc/fdfs/
      cp -f init.d/fdfs_trackerd /etc/rc.d/init.d/
      cp -f init.d/fdfs_storaged /etc/rc.d/init.d/
      /sbin/chkconfig --add fdfs_trackerd 
      /sbin/chkconfig --add fdfs_storaged
    fi
  fi
fi

