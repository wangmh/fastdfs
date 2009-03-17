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
make $1 $2

cd ../storage
cp Makefile.in Makefile
perl -pi -e "s#\\\$\(CFLAGS\)#$CFLAGS#g" Makefile
perl -pi -e "s#\\\$\(LIBS\)#$LIBS#g" Makefile
perl -pi -e "s#\\\$\(TARGET_PATH\)#$TARGET_PATH#g" Makefile
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
  cp restart.sh  /usr/local/bin/
fi

