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
