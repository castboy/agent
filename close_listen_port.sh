#/bin/bash

str=`netstat -lnp | grep $1`

OLD_IFS="$IFS"
IFS=" "
arr=($str)
#IFS="$OLD_IFS"
#for s in ${arr[@]}
#    do
#        echo $s
#    done


#OLD_IFS="$IFS"
IFS="/"
arr=(${arr[6]})
IFS="$OLD_IFS"
#for s in ${arr[@]}
#    do
#        echo $s
#    done

kill -9 ${arr[0]}
