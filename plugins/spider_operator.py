# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-8

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from fund_cn_etl.spider_api import query_job_status, login
from airflow.hooks.http_hook import HttpHook
from airflow.plugins_manager import AirflowPlugin


class SpiderJobSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, pre_task_id, token_hook_conn_id, api_hook_conn_id, *args, **kwargs):
        self.pre_task_id = pre_task_id
        self.token_hook = HttpHook(http_conn_id=token_hook_conn_id)

        self.api_hook = HttpHook(http_conn_id=api_hook_conn_id)
        self.token = None
        super(SpiderJobSensor, self).__init__(*args, **kwargs)

    def pre_execute(self, context):
        self.token = login(self.token_hook)

    def poke(self, context):
        ti = context['task_instance']
        job_ids = ti.xcom_pull(task_ids=self.pre_task_id, key='job_id_list')  # type:list

        for jid in job_ids:
            res = query_job_status(jid, self.api_hook, self.token)
            if 'errors' not in res and res['data'] != 'finished':
                return False
        return True


class SpiderPlugin(AirflowPlugin):
    name = "spider_plugin"
    operators = [SpiderJobSensor, ]
