# encoding: utf-8
# author TurboChang
#!/bin/bash

export ORACLE_SID=orcl
export ORACLE_HOME=/u01/app/oracle/product/12.2.0/db_1
export ORACLE_BASE=/u01/app/oracle
time=`date "+%Y%m%d"`
logdir=/u01/archcleanlog
#判断保存删除归档日志操作日志的目录是否存在
isDirectory(){
	if test -d $1
	then
		echo "$1目录存在!"
		else
			echo "$1目录不存在!,创建目录!"
			mkdir -p $1
	fi
}
#删除保存七天的删除归档日志操作日志
removeLog(){
	find $1 -type f -name "*.log" -mtime +7 -exec rm -rf {} \;
}
#删除7天前的归档日志
delete(){
	su - oracle -c "
	$ORACLE_HOME/bin/rman target /  <<EOF
	run{
	crosscheck archivelog all;
	delete noprompt expired archivelog all;
	DELETE noprompt ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-7';
	}
	exit
	EOF"
	echo "删除7天前的归档日志成功!"
}
log(){
	tee -a $logdir/archClean_$time.log
}
removeLog $logdir | log
isDirectory $logdir | log
delete | log
