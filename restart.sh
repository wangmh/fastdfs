#!/bin/sh

if [ -z "$1" ]; then
  /bin/echo "$0 <command line>"
  exit 1
fi

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

if [ -f /bin/expr ]; then
  EXPR=/bin/expr
else
  EXPR=/usr/bin/expr
fi

program=`/bin/echo $1 | $AWK -F '/' '{print $NF;}'`
param=''
grep_cmd="$GREP -w $program"

list='2 3 4 5 6 7 8 9'
for i in $list; do
  eval p='$'$i
  if [ -z "$p" ]; then
    break
  fi
  param="$param $p"
  grep_cmd="$grep_cmd | $GREP -w $p"
done
cmd="/bin/ps auxww | $grep_cmd | $GREP -v grep | $AWK '{print \$2;}'"
echo "$cmd"
pids=`$cmd`
if [ -z "$pids" ]; then
  i=0
  count=0
  /bin/echo "stopping $program ..."
  while [ 1 -eq 1 ]; do
    new_pids=''
    for pid in $pids; do
        if [ $i -eq 0 ]; then
           /bin/kill $pid
        else
           /bin/kill $pid >/dev/null 2>&1
        fi

        count=`$EXPR $count + 1`
    	if [ $? -eq 0 ]; then
           new_pids="$new_pids $pid"
    	fi
    done

    if [ -z "$new_pids" ]; then
      break
    fi

    pids="$new_pids"
    /usr/bin/printf .
    /bin/sleep 1
    i=`$EXPR $i + 1`
  done
fi

/bin/echo ""
count=`/bin/ps auxww | $grep_cmd | $GREP -v grep | $GREP -v $0 | /usr/bin/wc -l`
if [ $count -eq 0 ]; then
  /bin/echo "starting $program ..."
  exec $1 $param
else
  /bin/ps auxww | $grep_cmd | $GREP -v grep | $GREP -v $0
  /bin/echo "already running $program count: $count, restart aborted!"
fi

