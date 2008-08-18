#!/bin/sh

if [ -z "$1" ]; then
  /bin/echo "$0 <command line>"
  exit 1
fi

param=''
list='2 3 4 5 6 7 8 9'
for i in $list; do
  eval p='$'$i
  if [ -z $p ]; then
    break
  fi
  param="$param $p"
done

if [ -f /bin/awk ]; then
  AWK=/bin/awk
else
  AWK=/usr/bin/awk
fi

if [ -f /bin/grep ]; then
  GREP=/bin/grep
else
  GREP=/usr/bin/grep
fi

program=`/bin/echo $1 | $AWK -F '/' '{print $NF;}'`
count=`/bin/ps auxww | $GREP -w $program | $GREP -v grep | /usr/bin/wc -l`
i=0
if [ $count -gt 0 ]; then
  /bin/echo "stopping $program ..."
  while [ 1 -eq 1 ]; do
    if [ $i -eq 0 ]; then
       /usr/bin/killall $program
    else
       /usr/bin/killall $program >/dev/null 2>&1
    fi

    if [ $? -ne 0 ]; then
      break
    fi

    /usr/bin/printf .
    /bin/sleep 1
    let i=$i+1
  done
fi

/bin/echo ""
count=`/bin/ps auxww | $GREP -w $program | $GREP -v grep | $GREP -v $0 | /usr/bin/wc -l`
if [ $count -eq 0 ]; then
  /bin/echo "starting $program ..."
  exec $1 $param
else
  /bin/ps auxww | $GREP -w $program | $GREP -v grep | $GREP -v $0
  /bin/echo "already running $program count: $count, restart aborted!"
fi

