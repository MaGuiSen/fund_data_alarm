# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-5
from airflow.hooks.postgres_hook import PostgresHook


def query_latest_id(task_type, etl_hook: PostgresHook):
    """
    查询最新id
    :param task_type:
    :param etl_hook:
    :return:
    """

    la = etl_hook.get_first("select task_type,latest_id from cn_spider_snapshot where task_type=%s",
                            parameters=(task_type,))
    if la and la[1]:
        return la[1]


def update_latest_id(task_type, latest_id, etl_hook: PostgresHook):
    """
    更新最新ID
    :param task_type:
    :param latest_id:
    :param etl_hook:
    :return:
    """
    # 更新处理
    sql = """
    INSERT INTO cn_spider_snapshot
    (task_type,latest_id,updated_at)
    VALUES (%s,%s,LOCALTIMESTAMP)
    ON CONFLICT (task_type) DO UPDATE
    SET latest_id  = excluded.latest_id,
        updated_at = LOCALTIMESTAMP,
        pre_latest_id = cn_spider_snapshot.latest_id
    """
    etl_hook.run(sql, parameters=(task_type, latest_id))
