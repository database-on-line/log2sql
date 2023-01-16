import os
import sys
import argparse
import getpass
import datetime
import log2sql
import pymysql

def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.start_file:
        raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    start_time = vars(args)['--start-datetime']
    stop_time = vars(args)['--stop-datetime']
    if (start_time and not is_valid_datetime(start_time)) or \
            (stop_time and not is_valid_datetime(stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args


def parse_args():
    """parse args for binlog2sql"""
    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, nargs='*',
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', dest='--start-position', type=int,
                          help='Start position of the --start-file', default=0)
    interval.add_argument('--stop-position', dest='--stop-position', type=int,
                          help="Stop position. default: latest position of '--stop-file'", default=0)
    interval.add_argument('--start-datetime', dest='--start-datetime', type=str,
                          help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S", default='')
    interval.add_argument('--stop-datetime', dest='--stop-datetime', type=str,
                          help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;", default='')
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--database', '-d', dest='database', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                        help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                        help='Flashback data to start_position of start_file', default=False)
    parser.add_argument('--back-interval', dest='back_interval', type=float, default=1.0,
                        help="Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep")
    return parser


def get_data(**args_info):
    print(args_info)
    cmd_str = ["mysqlbinlog", "--base64-output=decode-rows  -vv"]
    for key, value in args_info.items():
        if key in ["--start-position", "--stop-position", "--start-datetime", "--stop-datetime", "start_file"]:
            if key == "start_file":
                cmd_str.append(value)
            else:
                if value != "" and value != 0:
                    if key in key in ["--start-position", "--stop-position"]:
                        list_info = "%s=%s" % (key, value)
                    else:
                        list_info = "%s='%s'" % (key, value)
                    cmd_str.insert(1, list_info)
    cmd_str = ' '.join(cmd_str)
    cmd_exec = os.popen(cmd_str)
    result_data = cmd_exec.readlines()
    return result_data



if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    conn = pymysql.Connect(**conn_setting)
    cur = conn.cursor() 
    result_data = get_data(**vars(args))
    log2sql.get_binlog(result_data, cur)
    conn.close()

