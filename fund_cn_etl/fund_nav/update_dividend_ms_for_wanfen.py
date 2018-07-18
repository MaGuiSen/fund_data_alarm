# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-8

import logging
import traceback

from airflow.hooks.postgres_hook import PostgresHook
from bson.objectid import ObjectId

from fund_cn_etl.mongo_plugin.hook.mongo_hook import MongoHook
from fund_cn_etl.service import etl_snapshot
from fund_cn_etl.service import log_service


def extract_div_amount(text):
    return float(text[5:-1])


def _clean_dividend(data):
    """
    分红数据清洗
    :return:
    """

    dividend_list = data['fhsp_detail']

    if 'data_list' not in dividend_list or len(dividend_list['data_list']) == 0:
        return []

    results = []
    for r in dividend_list['data_list']:
        divi_pay_date = r[4] if len(r) >= 5 and len(r[4]) > 0 else None
        divi_text = r[3]
        divi_per_share = extract_div_amount(divi_text)
        ex_divi_date = r[2]
        reg_date = r[1]
        year = r[0][:-1]
        results.append({'sec_id': data['fund_id'] + ".FCN", 'divi_pay_date': divi_pay_date, 'divi_text': divi_text,
                        'divi_per_share': divi_per_share,
                        'ex_divi_date': ex_divi_date, 'reg_date': reg_date, 'year': year
                        })

    return results


def _clean_split(data):
    """
    拆分数据清洗
    :return:
    """

    split_list = data['cfxq_detail']

    if 'data_list' not in split_list or len(split_list['data_list']) == 0:
        return []

    results = []

    for r in split_list['data_list']:

        split_date = r[1]
        split_type = r[2]
        split_desc = r[3]

        if split_desc not in ('暂未披露'):
            nums = split_desc.split(u':')
            if len(nums) != 2:
                logging.error('解析比例出错：%s', split_desc)
                raise Exception()
            ratio = float(nums[1]) / float(nums[0])
        else:
            ratio = None

        year = r[0][:-1]
        results.append(
            {'sec_id': data['fund_id'] + ".FCN", 'split_date': split_date, 'split_type': split_type, 'ratio': ratio,
             'split_desc': split_desc, 'year': year,
             })
    return results


def _upsert_dividend(records, pg_hook):
    if len(records) == 0:
        return

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
            insert into fund_dividen(sec_id, year, reg_date, ex_divi_date, divi_pay_date, divi_per_share, divi_text,updated_at)  
            values(%(sec_id)s,%(year)s,%(reg_date)s,%(ex_divi_date)s,%(divi_pay_date)s,%(divi_per_share)s,%(divi_text)s,LOCALTIMESTAMP)
            on conflict(sec_id,reg_date) do nothing;
            """
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()


def _upsert_split(records, pg_hook):
    if len(records) == 0:
        return

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
            insert into fund_split(sec_id, year, split_date, split_type,ratio,split_desc,updated_at)  
            values(%(sec_id)s,%(year)s,%(split_date)s,%(split_type)s,%(ratio)s,%(split_desc)s,LOCALTIMESTAMP)
            on conflict(sec_id,split_date) do nothing;
            """
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()


def update_dividend_and_split(spider_conn_id, fund_core_cn_conn_id, etl_conn_id):
    spider_hook = MongoHook(conn_id=spider_conn_id)
    pg_hook = PostgresHook(postgres_conn_id=fund_core_cn_conn_id)
    etl_hook = PostgresHook(postgres_conn_id=etl_conn_id)
    task_type = 'ms_fund_dividen_for_wanfen'
    query = {}
    last_id = etl_snapshot.query_latest_id(task_type, etl_hook)
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}

    results = spider_hook.find(task_type, query, batch_size=500)
    logging.info("数据加载完成")
    succeed = 0
    failed = 0

    for r in results:
        try:
            dividend = _clean_dividend(r['data'])
            if dividend:
                _upsert_dividend(dividend, pg_hook)
            split = _clean_split(r['data'])
            if split:
                _upsert_split(split, pg_hook)

            etl_snapshot.update_latest_id(task_type, str(r['_id']), etl_hook)
            succeed += 1
        except:
            failed += 1
            logging.error("处理出错，跳过 %s", r['_id'])
            log_service.add_log({
                "task": "update_dividend_and_split",
                "collection": task_type,
                "data_id": str(r['_id']),
                "exception": traceback.format_exc()
            }, etl_hook)
            # logging.error(traceback.format_exc())

    logging.info("更新完成,成功更新[%s]个分红送配，[%s]个失败", succeed, failed)


if __name__ == '__main__':
    data = {
        "fund_id": "110005",
        "cfxq_detail": {
            "data_list": [
                [
                    "2007年",
                    "2007-02-07",
                    "份额折算",
                    "1:2.5212"
                ]
            ],
            "title_list": [
                "年份",
                "拆分折算日",
                "拆分类型",
                "拆分折算比例"
            ]
        },
        "fhsp_detail": {
            "data_list": [
                [
                    "2018年",
                    "2018-03-16",
                    "2018-03-16",
                    "每份派现金0.0330元",
                    "2018-03-19"
                ],
                [
                    "2018年",
                    "2018-02-08",
                    "2018-02-08",
                    "每份派现金0.0300元",
                    "2018-02-09"
                ],
                [
                    "2018年",
                    "2018-01-15",
                    "2018-01-15",
                    "每份派现金0.0400元",
                    "2018-01-16"
                ],
                [
                    "2016年",
                    "2016-01-13",
                    "2016-01-13",
                    "每份派现金0.1650元",
                    "2016-01-14"
                ],
                [
                    "2014年",
                    "2014-03-21",
                    "2014-03-21",
                    "每份派现金0.0400元",
                    "2014-03-24"
                ],
                [
                    "2014年",
                    "2014-03-07",
                    "2014-03-07",
                    "每份派现金0.0400元",
                    "2014-03-10"
                ],
                [
                    "2013年",
                    "2013-03-15",
                    "2013-03-15",
                    "每份派现金0.0400元",
                    "2013-03-18"
                ],
                [
                    "2013年",
                    "2013-03-01",
                    "2013-03-01",
                    "每份派现金0.0400元",
                    "2013-03-04"
                ],
                [
                    "2011年",
                    "2011-03-09",
                    "2011-03-09",
                    "每份派现金0.0500元",
                    "2011-03-10"
                ],
                [
                    "2011年",
                    "2011-02-10",
                    "2011-02-10",
                    "每份派现金0.0400元",
                    "2011-02-11"
                ],
                [
                    "2010年",
                    "2010-11-22",
                    "2010-11-22",
                    "每份派现金0.0300元",
                    "2010-11-23"
                ],
                [
                    "2010年",
                    "2010-08-10",
                    "2010-08-10",
                    "每份派现金0.0500元",
                    "2010-08-11"
                ],
                [
                    "2010年",
                    "2010-03-11",
                    "2010-03-11",
                    "每份派现金0.0800元",
                    "2010-03-12"
                ],
                [
                    "2010年",
                    "2010-02-02",
                    "2010-02-02",
                    "每份派现金0.0700元",
                    "2010-02-03"
                ],
                [
                    "2009年",
                    "2009-12-24",
                    "2009-12-24",
                    "每份派现金0.0700元",
                    "2009-12-25"
                ],
                [
                    "2008年",
                    "2008-04-10",
                    "2008-04-10",
                    "每份派现金0.1000元",
                    "2008-04-11"
                ],
                [
                    "2007年",
                    "2007-12-21",
                    "2007-12-21",
                    "每份派现金0.0500元",
                    "2007-12-24"
                ],
                [
                    "2007年",
                    "2007-09-10",
                    "2007-09-10",
                    "每份派现金0.0500元",
                    "2007-09-11"
                ],
                [
                    "2007年",
                    "2007-05-08",
                    "2007-05-08",
                    "每份派现金0.0500元",
                    "2007-05-09"
                ],
                [
                    "2006年",
                    "2006-11-21",
                    "2006-11-21",
                    "每份派现金0.1000元",
                    "2006-11-23"
                ],
                [
                    "2006年",
                    "2006-05-24",
                    "2006-05-24",
                    "每份派现金0.0200元",
                    "2006-05-26"
                ],
                [
                    "2006年",
                    "2006-05-19",
                    "2006-05-19",
                    "每份派现金0.0900元",
                    "2006-05-23"
                ],
                [
                    "2006年",
                    "2006-03-30",
                    "2006-03-30",
                    "每份派现金0.0300元",
                    "2006-04-03"
                ],
                [
                    "2005年",
                    "2005-08-23",
                    "2005-08-23",
                    "每份派现金0.0200元",
                    "2005-08-25"
                ]
            ],
            "title_list": [
                "年份",
                "权益登记日",
                "除息日",
                "每份分红",
                "分红发放日"
            ]
        }
    }

    import pprint

    pprint.pprint(_clean_dividend(data))
    pprint.pprint(_clean_split(data))
