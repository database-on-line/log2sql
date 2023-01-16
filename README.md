# log2sql
for offline binlog,need server get schema
背景:

   因为binlog2sql无法解析离线的binlog，针对rds来说，binlog线上保留时间不长，
   所以需要一个能够离线解析binlog的脚本
   
权限:

   为了获取表结构需要有对information_schema.STATISTICS，information_schema.columns的读取权限
    
使用示例:

   python3 main.py --start-position=4 --stop-position=1024  --start-file mysql-bin.000057 -h 127.0.0.1 --port 3306 -uroot -p
   python3 main.py --start-position=4 --stop-position=1024  --start-file mysql-bin.000057 -h 127.0.0.1 --port 3306 -uroot -p --database='log_test' -t 'a' 'd'   
