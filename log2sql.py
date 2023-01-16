import sys
import os
import pymysql 

def format_sql(sql_list, cur, args):
    dml_all = args.sql_type
    flashback = args.flashback
    database = args.database
    tables = args.tables
    for sql_info_all in sql_list:
        dml_type = ""
        for sql_info in sql_info_all:
            if "@" != sql_info.strip()[0]:
                if "insert" in sql_info.lower() or  "update" in sql_info.lower() or "delete" in sql_info.lower():
                    info_new = sql_info.strip().split(" ")
                    dml_type = info_new[0].lower()
                    schema_info = info_new[-1]
                    index_info = {}
                    db_name = schema_info.split(".")[0].strip('`')
                    table_name = schema_info.split(".")[1].strip('`')
                    if dml_type == "update":
                        column_info = {"before_values": {}, "after_values": {}}
                    else:
                        column_info = {"values": {}}
                    cur.execute("select COLUMN_NAME,ORDINAL_POSITION,COLUMN_KEY from information_schema.columns where table_schema='%s' and table_name='%s'" % (db_name, table_name))
                    table_info_all = cur.fetchall()
                    column_dict = {}
                    key_dict = {}
                    for table_info in table_info_all:
                        if table_info[2] == "PRI":
                            column_dict[table_info[1]] = table_info[0]
                            key_dict[table_info[1]] = table_info[0]
                        else:
                            column_dict[table_info[1]] = table_info[0]
                    if key_dict == {}:
                        cur.execute(" select column_name from information_schema.STATISTICS where index_name = (select index_name \
                            from information_schema.STATISTICS where table_schema='%s' and table_name='%s' and non_unique=0 limit 1) \
                            and table_schema='%s' and table_name='%s';" % (db_name, table_name, db_name, table_name))
                        key_info_all = [row[0] for row in cur.fetchall()]
                        for key,value in column_dict.items():
                            if value in key_info_all:
                                key_dict[key] = value
            else:
                aaa = sql_info.strip().split("=")
                info_key = column_dict[int(aaa[0].replace("@", ""))]
                if dml_type == "update":
                    if info_key not in column_info["before_values"].keys():
                        column_info["before_values"][info_key] = aaa[1]
                    else:
                        column_info["after_values"][info_key] = aaa[1]
                else:
                    column_info["values"][info_key] = aaa[1]
                if info_key in key_dict.values():
                    index_info[info_key] = aaa[1]
        if dml_type in dml_all or dml_all == []:
            if database == "" or db_name == database:
                if tables == "" or table_name in tables:
                    generate_sql_pattern(dml_type, column_info=column_info, flashback=flashback, index_info=index_info, schema_info=schema_info)


def get_binlog(result_data, cur, args):
    # 提取start_pos,end_pos等操作稍后考虑
    list_sql = []
    num = 0
    is_insert = 0
    for binlog_info in result_data:
        #if is_insert == 1:
        #    list_sql[num].append(binlog_info)
        if "###" == binlog_info[:3]:
            binlog_new_info = binlog_info.replace("###", "").replace("\n", "").split("/*")[0]
            if "insert" in binlog_new_info.lower() or "delete" in binlog_new_info.lower() or "update" in binlog_new_info.lower():
                list_sql.append([])
                num += 1
            list_sql[num-1].append(binlog_new_info)
            is_insert = 1
        elif "###" != binlog_info[:3] and is_insert ==1:
            is_insert = 0
            list_sql.append([])
            num += 1
    list_sql = list_sql[:-1]
    format_sql(list_sql, cur, args)


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if sys.version > '3':
        PY3PLUS = True
    else:
        PY3PLUS = False
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def generate_sql_pattern(dml_type, column_info=None, flashback=False, index_info=None, schema_info=None):
    template = ''
    if flashback is True:
        if dml_type == "insert":
            if index_info != {}:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in index_info.items()])
            else:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in column_info['values'].items()])
            template = 'DELETE FROM {0} WHERE {1} LIMIT 1;'.format(
                schema_info, filter_criteria)
        elif dml_type == "delete":
            template = 'INSERT INTO {0}({1}) VALUES ({2});'.format(
                schema_info,
                ', '.join(map(lambda key: '`%s`' % key, column_info['values'].keys())),
                ', '.join(column_info['values'].values())
            )
        elif dml_type == "update":
            if index_info != {}:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in index_info.items()])
            else:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in column_info['after_values'].items()])
            template = 'UPDATE {0} SET {1} WHERE {2} LIMIT 1;'.format(
                schema_info,
                ', '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in column_info['before_values'].items()]),
                filter_criteria)
    else:
        if dml_type == "insert":
            template = 'INSERT INTO {0}({1}) VALUES ({2});'.format(
                schema_info,
                ', '.join(map(lambda key: '`%s`' % key, column_info['values'].keys())),
                ', '.join(column_info['values'].values())
            )
        elif dml_type == "delete":
            if index_info != {}:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in index_info.items()])
            else:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in column_info['values'].items()])
            template = 'DELETE FROM {0} WHERE {1} LIMIT 1;'.format(
                schema_info, filter_criteria)
        elif dml_type == "update":
            if index_info != {}:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in index_info.items()])
            else:
                filter_criteria = ' AND '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in column_info['before_values'].items()])
            template = 'UPDATE {0} SET {1} WHERE {2} LIMIT 1;'.format(
                schema_info,
                ', '.join(['`%s`=%s' % (x,fix_object(y)) for x,y in column_info['after_values'].items()]),
                filter_criteria)
    print(template)



