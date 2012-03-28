#!/bin/bash
remote_base="/tmp/.mercurial"
virt=/home/admin/virt/bin/activate
remote_hg=/home/tops/bin/hg
project=sitebase
host=$1

if [ "$host" == "localhost" ]; then
	if [ -z "$project" ]; then
	  echo "project is empty"
	  exit
	fi
	source $virt
	[ -e "$remote_base/$project" ] && rm -rf $remote_base/$project
	mkdir -p $remote_base/$project
	cd $remote_base/$project
	$remote_hg init
elif [ -n "$host" ]; then
	scp $0 $host:/tmp
	ssh $host "$(basename $0) localhost"
	hg push --remotecmd $remote_hg ssh://$host/$remote_base/$project
else
	echo "usage: $0 hostname"
	exit
fi
