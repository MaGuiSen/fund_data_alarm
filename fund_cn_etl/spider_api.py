# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-7

import json
import logging
from hashlib import sha256

from airflow.hooks.http_hook import HttpHook


def login(http_hook: HttpHook):
    conn = http_hook.get_connection(http_hook.http_conn_id)
    query_tpl = """
        mutation {
          login(loginName: "%s", password: "%s", from: api)
        }
    """
    data = {
        'query': query_tpl % (conn.login, sha256(conn.password.encode('utf8')).hexdigest())}
    res = http_hook.run("ucenter/api/graphql/index", data=data, headers={'Accept': "application/json"})
    # logging.info(data)
    if 'errors' in res:
        logging.error(res['errors'])
        raise Exception(res['error'])
    res = json.loads(res.content)
    return res['data']['login']


def run_job(project, spider, spider_hook: HttpHook, token, instance_num=1, setting=None):
    data = {
        "project": project,
        'spider': spider,
        'setting': setting,
        'node_size': instance_num
    }
    resp = spider_hook.run("spider/api/run-schedule?access_token=" + token, data=data,
                           headers={'Accept': "application/json"})

    return json.loads(resp.content)


def query_job_status(job_id, http_hook: HttpHook, token):
    session = http_hook.get_conn(headers={'Accept': "application/json"})
    res = session.get(url=http_hook.base_url + "spider/api/job-status",
                      params={'access_token': token, 'job_id': job_id})
    return json.loads(res.content)


def start_spider(project, spider, token_hook_conn_id, api_hook_conn_id, instance_num=1, **context):
    """
    启动爬虫
    :param project:
    :param spider:
    :param token_hook_conn_id:
    :param api_hook_conn_id:
    :param instance_num:
    :param context:
    :return:
    """
    token_hook = HttpHook(http_conn_id=token_hook_conn_id)
    api_hook = HttpHook(http_conn_id=api_hook_conn_id)

    token = login(token_hook)
    res = run_job(project, spider, api_hook, token, instance_num=instance_num)

    if 'errors' in res:
        raise Exception(res['errors'])
    job_id_list = res['data']
    context['task_instance'].xcom_push(key='job_id_list', value=job_id_list)


def add_tasks(tasks, task_hook_conn_id):
    task_hook = HttpHook(http_conn_id=task_hook_conn_id)
    data = {
        'tasks': tasks
    }
    logging.info("增加[%s]个任务", len(tasks))
    conn = task_hook.get_connection(task_hook.http_conn_id)
    session = task_hook.get_conn(headers={})
    res = session.post(url=task_hook.base_url + "task/addbatch", json=data,
                       headers={'Content-Type': 'application/json', 'spider-client-id': conn.login,
                                'spider-api-key': conn.password})
    res = json.loads(res.content)
    succeed = isinstance(res, list) and len(res) > 0

    if not succeed:
        raise Exception(res)


def test(token_hook_conn_id, api_hook_conn_id):
    token_hook = HttpHook(http_conn_id=token_hook_conn_id)
    api_hook = HttpHook(http_conn_id=api_hook_conn_id)

    token = login(token_hook)
    logging.debug("登录获取的token:%s", token)

    res = run_job('fund_cn', 'tt_fund_bank_list', api_hook, token)

    job_id = res['data']
    logging.info("启动作用，id为%s", job_id)

    res = query_job_status(job_id, api_hook, token)
    logging.info("查询作业状态：%s", res)
