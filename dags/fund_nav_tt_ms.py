# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-8
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.spider_plugin import SpiderJobSensor
from fund_cn_etl.fund_nav import fund_nav_std, fund_nav_rtn, fund_currency_std, fund_currency_rtn
from fund_cn_etl.fund_nav import update_dividend
from fund_cn_etl.fund_nav import update_fund_nav_raw
from fund_cn_etl.service.task_service import create_all_nav_task
from fund_cn_etl.service.task_service import create_dividend_task
from fund_cn_etl.spider_api import add_tasks, start_spider

local_tz = pytz.timezone("Asia/Shanghai")

default_args = {
    'owner': "Brandon",
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=2),
    'email': ['linbingduan@1ping.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3

}

dag = DAG('fund_daily_rtn', default_args=default_args, catchup=False,
          max_active_runs=1, description="基金日收益更新", schedule_interval="0 9,16 * * *")  # '0 8 * * * '

# NAV爬虫及其ETL
biz_group = "TT_FUND"
task_type = "tt_fund_lishi_jingzhi_space_list"
add_update_tasks = PythonOperator(task_id='add_list_task',
                                  python_callable=add_tasks,
                                  op_kwargs={
                                      'tasks': create_all_nav_task("fund_core_cn", task_type, biz_group),
                                      'task_hook_conn_id': 'http_spider_task'},
                                  dag=dag)

start_nav_spider = PythonOperator(task_id='start_nav_spider',
                                  python_callable=start_spider,
                                  op_kwargs={"project": "fund_cn",
                                             "spider": task_type,
                                             "token_hook_conn_id": "http_spider_token",
                                             "api_hook_conn_id": "http_spider_api",
                                             "instance_num": 3},
                                  provide_context=True,
                                  dag=dag)

wait_nav_spider_to_finished = SpiderJobSensor(task_id='wait_nav_spider_to_finished',
                                              poke_interval=60,
                                              pre_task_id="start_nav_spider",
                                              token_hook_conn_id="http_spider_token",
                                              api_hook_conn_id="http_spider_api", dag=dag)

op_params = {
    'spider_conn_id': 'mongo_spider',
    'fund_core_cn_conn_id': 'fund_core_cn',
    'etl_conn_id': 'fund_etl'}

update_raw_nav = PythonOperator(task_id='update_raw_nav',
                                python_callable=update_fund_nav_raw.update_raw_nav,
                                op_kwargs=op_params,
                                dag=dag)

add_update_tasks >> start_nav_spider

start_nav_spider >> wait_nav_spider_to_finished

wait_nav_spider_to_finished >> update_raw_nav

# 分红分拆爬虫以及ETL
div_task_type = "tt_fund_fenhong_songpei"

add_div_split_list_task = PythonOperator(task_id='add_div_split_list_task',
                                         python_callable=add_tasks,
                                         op_kwargs={
                                             'tasks': create_dividend_task("fund_core_cn", div_task_type, biz_group),
                                             'task_hook_conn_id': 'http_spider_task'},
                                         dag=dag)

start_dividend_split_spider = PythonOperator(task_id='start_dividend_split_spider',
                                             python_callable=start_spider,
                                             op_kwargs={"project": "fund_cn", "spider": div_task_type,
                                                        "token_hook_conn_id": "http_spider_token",
                                                        "api_hook_conn_id": "http_spider_api",
                                                        "instance_num": 3},
                                             provide_context=True,
                                             dag=dag)

wait_div_split_spider_to_finished = SpiderJobSensor(task_id='wait_div_split_spider_to_finished',
                                                    poke_interval=60,
                                                    pre_task_id="start_dividend_split_spider",
                                                    token_hook_conn_id="http_spider_token",
                                                    api_hook_conn_id="http_spider_api",
                                                    dag=dag)

op_params = {
    'spider_conn_id': 'mongo_spider',
    'fund_core_cn_conn_id': 'fund_core_cn',
    'etl_conn_id': 'fund_etl'}

update_dividend_split = PythonOperator(task_id='update_dividend_and_split',
                                       python_callable=update_dividend.update_dividend_and_split,
                                       op_kwargs=op_params,
                                       dag=dag)

add_div_split_list_task >> start_dividend_split_spider

start_dividend_split_spider >> wait_div_split_spider_to_finished

wait_div_split_spider_to_finished >> update_dividend_split

# 分拆和分红在NAV之前更新，以便使用NAV中的更准确的数据进行更新

update_dividend_split >> update_raw_nav

op_params = {
    'fund_core_cn_conn_id': 'fund_core_cn',
    'etl_conn_id': 'fund_etl'}

std_task = PythonOperator(task_id='nav_std',
                          python_callable=fund_nav_std.standarlize_nav,
                          op_kwargs=op_params,
                          dag=dag)

update_daily_rtn = PythonOperator(task_id='update_daily_rtn',
                                  python_callable=fund_nav_rtn.update_nav_daily_rtn,
                                  op_kwargs=op_params,
                                  dag=dag)
std_currency_task = PythonOperator(task_id='std_currency_task',
                                   python_callable=fund_currency_std.standarlize_currency_rtn,
                                   op_kwargs=op_params,
                                   dag=dag)

update_currency_daily_rtn = PythonOperator(task_id='update_currency_daily_rtn',
                                           python_callable=fund_currency_rtn.update_cur_daily_rtn,
                                           op_kwargs=op_params,
                                           dag=dag)

# 标准化几日收益更新
update_raw_nav >> std_task
std_task >> update_daily_rtn

update_raw_nav >> std_currency_task
std_currency_task >> update_currency_daily_rtn


