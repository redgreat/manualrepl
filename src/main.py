#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/21 15:26
# comment: 监听mysql binglog，监听变化量后写入宽表

import sys
import os
import configparser
from loguru import logger

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

# 数据库连接定义
config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)

my_host = config.get("master", "host")
my_database = config.get("master", "database").split(',')
my_tables = config.get("master", "tables").split(',')
my_user = config.get("master", "user")
my_password = config.get("master", "password")
my_port = int(config.get("master", "port"))


# 日志配置
logDir = os.path.expanduser("../log/")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "repl.log")
# logger.remove(handler_id=None)

logger.add(
    logFile,
    colorize=True,
    rotation="1 days",
    retention="3 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True,
    level="INFO",
)

MYSQL_SETTINGS = {
    "host": my_host,
    "port": 3306,
    "user": my_user,
    "passwd": my_password,
    "charset": "utf8",
}


def main():
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=3,
        blocking=True,  # 持续监听
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],  # 指定只监听某些事件
        only_schemas=my_database,  # 指定只监听某些库（但binlog还是要读取全部滴）
    )

    for binlog_event in stream:
        for row in binlog_event.rows:
            event = {"schema": binlog_event.schema, "table": binlog_event.table}

            if isinstance(binlog_event, DeleteRowsEvent):
                event["action"] = "delete"
                event.update(row["values"])
            elif isinstance(binlog_event, UpdateRowsEvent):
                event["action"] = "update"
                event.update(row["after_values"])
            elif isinstance(binlog_event, WriteRowsEvent):
                event["action"] = "insert"
                event.update(row["values"])
            print(event)
            sys.stdout.flush()

    stream.close()


if __name__ == "__main__":
    main()
