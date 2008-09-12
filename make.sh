tmp_src_filename=fdfs_check_bits.c
cat <<EOF > $tmp_src_filename
#include <stdio.h>
int main()
{
	printf("%d\n", sizeof(long));
	return 0;
}
EOF

cc $tmp_src_filename
bytes=`./a.out`

/bin/rm -f  a.out $tmp_src_filename
if [ "$bytes" -eq 8 ]; then
 OS_BITS=64
else
 OS_BITS=32
fi

cat <<EOF > common/fdfs_os_bits.h
#ifndef _FDFS_OS_BITS
#define _FDFS_OS_BITS

#define OS_BITS  $OS_BITS

#endif
EOF

cd tracker
make $1 $2

cd ../storage
make $1 $2

cd ../client
make $1 $2

if [ "$1" = "install" ]; then
  cd ..
  cp restart.sh  /usr/local/bin/
fi
