# -*- coding: utf-8 -*-


# Created by Brandon on 7/9/2018
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.spider_plugin import SpiderJobSensor
from plugins.spider_operator import SpiderJobSensor


from fund_cn_etl.spider_api import add_tasks, start_spider
from fund_cn_etl.fund_nav import update_dividend_ms
from datetime import datetime, timedelta

"""
晨星分红拆分数据抓取dag
"""

default_args = {
    "owner": "Brandon",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=2),
    "email": ["linbingduan@1ping.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
}


def construct_dividend_subdag(parent_dag_name, child_dag_name, args, schedule_interval):
    subdag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        catchup=False,
        default_args=args,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        description="晨星分红和拆分抓取",
    )

    add_div_split_list_task = PythonOperator(
        task_id="add_div_split_list_task",
        python_callable=add_tasks,
        op_kwargs={
            "tasks": [
                {
                    "biz_group": "fund_core_cn",
                    "task_type": "ms_fund_list",
                    "params": {"ms_fund_dividen": "1"},
                }
            ],
            "task_hook_conn_id": "http_spider_task",
        },
        dag=subdag,
    )

    start_dividend_list_spider = PythonOperator(
        task_id="start_dividend_split_list_spider",
        python_callable=start_spider,
        op_kwargs={
            "project": "fund_cn",
            "spider": "ms_fund_list",
            "token_hook_conn_id": "http_spider_token",
            "api_hook_conn_id": "http_spider_api",
            "instance_num": 3,
        },
        provide_context=True,
        dag=subdag,
    )

    wait_list_spider_to_finished = SpiderJobSensor(
        task_id="wait_div_split_list_to_finished",
        poke_interval=60,
        pre_task_id="start_dividend_split_list_spider",
        token_hook_conn_id="http_spider_token",
        api_hook_conn_id="http_spider_api",
        dag=subdag,
    )

    start_dividend_split_spider = PythonOperator(
        task_id="start_dividend_split_detail_spider",
        python_callable=start_spider,
        op_kwargs={
            "project": "fund_cn",
            "spider": "ms_fund_dividen",
            "token_hook_conn_id": "http_spider_token",
            "api_hook_conn_id": "http_spider_api",
            "instance_num": 3,
        },
        provide_context=True,
        dag=subdag,
    )

    wait_div_split_spider_to_finished = SpiderJobSensor(
        task_id="wait_div_split_spider_to_finished",
        poke_interval=60,
        pre_task_id="start_dividend_split_detail_spider",
        token_hook_conn_id="http_spider_token",
        api_hook_conn_id="http_spider_api",
        dag=subdag,
    )

    op_params = {
        'spider_conn_id': 'mongo_spider',
        'fund_core_cn_conn_id': 'fund_core_cn',
        'etl_conn_id': 'fund_etl'}

    update_dividend_split = PythonOperator(task_id='update_dividend_and_split',
                                           python_callable=update_dividend_ms.update_dividend_and_split,
                                           op_kwargs=op_params,
                                           dag=subdag)

    add_div_split_list_task >> start_dividend_list_spider
    start_dividend_list_spider >> wait_list_spider_to_finished
    wait_list_spider_to_finished >> start_dividend_split_spider
    start_dividend_split_spider >> wait_div_split_spider_to_finished
    wait_div_split_spider_to_finished >> update_dividend_split
    return subdag


dag = construct_dividend_subdag("", "ms_dividend_split_crawler", default_args, None)