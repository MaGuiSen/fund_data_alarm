# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-11

from airflow.hooks.postgres_hook import PostgresHook

_add_log_sql = """
    insert into error_log (task,collection,data_id,exception,updated_at)
    values (%(task)s,%(collection)s,%(data_id)s,%(exception)s,LOCALTIMESTAMP)
    """


def add_log(log, pg_hook: PostgresHook):
    """
    新增日志
    :param log:
    :param pg_hook:
    :return:
    """
    assert log is not None, "log对象不能为None"
    pg_hook.run(_add_log_sql, parameters=log)
