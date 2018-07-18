# -*- coding: utf-8 -*-


# Created by qeelyn on 18-5-8
import logging
import traceback
import datetime as dt
from bson.objectid import ObjectId
from airflow.hooks.postgres_hook import PostgresHook
from fund_cn_etl.mongo_plugin.hook.mongo_hook import MongoHook
from fund_cn_etl.service import etl_snapshot
from fund_cn_etl.service import log_service


def extract_div_amount(text):
    return float(text[5:-1])


def extract_date(timestamp_str):
    # /Date(1448208000000+0800)/
    if timestamp_str:
        return dt.date.fromtimestamp(
            int(timestamp_str.replace("/Date(", "").replace("000+0800)/", ""))
        )
    return None


def _clean_dividend(data):
    """
    分红数据清洗
    :return:
    """
    if (
            "detail" not in data
            or "Dividend" not in data["detail"]
            or not data["detail"]["Dividend"]
    ):
        return []

    dividend_list = data["detail"]["Dividend"]

    results = []
    for dividend in dividend_list:
        # 每份分红
        divi_per_share = dividend.get("TotalDistribution", None)
        # 除息日
        ex_divi_date = extract_date(dividend.get("ExcludingDate", None))
        # 再投资日期
        reinvest_date = extract_date(dividend.get("ReinvestDate", None))
        # 再投资价格
        reinvest_price = dividend.get("ReinvestPrice", None)
        # 分红发放日
        divi_pay_date = None
        # 分红派送文本
        divi_text = None
        # 权益登记日
        reg_date = None
        # 分红年份
        year = None
        if ex_divi_date:
            year = ex_divi_date.year
        results.append(
            {
                "sec_id": data["fund_id"] + ".FCN",
                "divi_pay_date": divi_pay_date,
                "divi_text": divi_text,
                "divi_per_share": divi_per_share,
                "ex_divi_date": ex_divi_date,
                "reg_date": reg_date,
                "year": year,
                "reinvest_date": reinvest_date,
                "reinvest_price": reinvest_price,
                "remark": "ms",
            }
        )

    return results


def _clean_split(data):
    """
    拆分数据清洗
    :return:
    """
    if (
            "detail" not in data
            or "Split" not in data["detail"]
            or not data["detail"]["Split"]
    ):
        return []

    split_list = data["detail"]["Split"]

    results = []
    for split in split_list:
        # 拆分日期
        split_date = extract_date(split.get("EffectiveDate", None))
        # 拆分类型
        split_type = None
        # 拆分描述
        split_desc = None
        # 拆分比例
        ratio = split.get("Ratio")
        year = None
        if split_date:
            year = split_date.year
        results.append(
            {
                "sec_id": data["fund_id"] + ".FCN",
                "split_date": split_date,
                "split_type": split_type,
                "ratio": ratio,
                "split_desc": split_desc,
                "year": year,
                "remark": "ms"
            }
        )
    return results


def _upsert_dividend(records, pg_hook):
    if len(records) == 0:
        return

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
            insert into fund_dividen(sec_id, year, reg_date, ex_divi_date, divi_pay_date, divi_per_share, divi_text,reinvest_date,reinvest_price,remark,updated_at)  
            values(%(sec_id)s,%(year)s,%(reg_date)s,%(ex_divi_date)s,%(divi_pay_date)s,%(divi_per_share)s,%(divi_text)s,%(reinvest_date)s,%(reinvest_price)s,%(remark)s,LOCALTIMESTAMP)
            on conflict(sec_id,reg_date) do update 
            set updated_at=EXCLUDED.updated_at,
            ex_divi_date=EXCLUDED.ex_divi_date,
            divi_per_share = EXCLUDED.divi_per_share;
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
            insert into fund_split(sec_id, year, split_date, split_type,ratio,split_desc,remark,updated_at)  
            values(%(sec_id)s,%(year)s,%(split_date)s,%(split_type)s,%(ratio)s,%(split_desc)s,%(remark)s,LOCALTIMESTAMP)
            on conflict(sec_id,split_date) do update 
            set ratio=EXCLUDED.ratio,
            updated_at = EXCLUDED.updated_at;
            """
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()


def update_dividend_and_split(spider_conn_id, fund_core_cn_conn_id, etl_conn_id):
    spider_hook = MongoHook(conn_id=spider_conn_id)
    pg_hook = PostgresHook(postgres_conn_id=fund_core_cn_conn_id)
    etl_hook = PostgresHook(postgres_conn_id=etl_conn_id)
    task_type = "ms_fund_dividen"
    query = {}
    last_id = etl_snapshot.query_latest_id(task_type, etl_hook)
    if last_id:
        query["_id"] = {"$gt": ObjectId(last_id)}

    results = spider_hook.find(task_type, query, batch_size=200)
    logging.info("数据加载完成")
    succeed = 0
    failed = 0

    for r in results:
        try:
            dividend = _clean_dividend(r["data"])
            if dividend:
                _upsert_dividend(dividend, pg_hook)
            split = _clean_split(r["data"])
            if split:
                _upsert_split(split, pg_hook)

            etl_snapshot.update_latest_id(task_type, str(r["_id"]), etl_hook)
            succeed += 1
        except:
            failed += 1
            logging.error("处理出错，跳过 %s", traceback.format_exc())
            logging.error("处理出错，跳过 %s", r["_id"])
            log_service.add_log(
                {
                    "task": "update_dividend_and_split",
                    "collection": task_type,
                    "data_id": str(r["_id"]),
                    "exception": traceback.format_exc(),
                },
                etl_hook,
            )
            # logging.error(traceback.format_exc())
    logging.info("更新完成,成功更新[%s]个分红送配，[%s]个失败", succeed, failed)


if __name__ == "__main__":
    data = {
        "fund_id": "110005",
        "detail": {
            "Dividend": [
                {
                    "TotalDistribution": 0.1,
                    "ReinvestDate": "/Date(1513094400000+0800)/",
                    "ExcludingDate": "/Date(1513094400000+0800)/",
                    "ReinvestPrice": 13.291,
                    "FundClassId": "F0000003WH",
                },
                {
                    "TotalDistribution": 0.1,
                    "ReinvestDate": "/Date(1481558400000+0800)/",
                    "ExcludingDate": "/Date(1481558400000+0800)/",
                    "ReinvestPrice": 10.194,
                    "FundClassId": "F0000003WH",
                },
                {
                    "TotalDistribution": 0.1,
                    "ReinvestDate": "/Date(1448208000000+0800)/",
                    "ExcludingDate": "/Date(1448208000000+0800)/",
                    "ReinvestPrice": 11.524,
                    "FundClassId": "F0000003WH",
                },
            ],
            "Split": [
                {
                    "Ratio": 0.9714,
                    "EffectiveDate": "/Date(1525622400000+0800)/",
                    "FundClassId": "0P0000YKEQ",
                },
                {
                    "Ratio": 0.98626,
                    "EffectiveDate": "/Date(1493913600000+0800)/",
                    "FundClassId": "0P0000YKEQ",
                },
            ],
        },
    }

    import pprint

    dividend = _clean_dividend(data)
    split = _clean_split(data)
    pprint.pprint(dividend)
    pprint.pprint(split)
