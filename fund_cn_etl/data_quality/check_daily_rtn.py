# -*- coding: utf-8 -*-
import requests
import logging
from scrapy import Selector
import demjson
import time
import datetime as dt
import re
from airflow.hooks.postgres_hook import PostgresHook


def check_update_rtn(fund_core_cn_conn_id):
    fund_core_cn_hook = PostgresHook(postgres_conn_id=fund_core_cn_conn_id)
    error_list_sql = 'select * from fund_daily_rtn WHERE abs(rtn) > 0.2'
    error_list = [{'id': r[0], 'sec_id': r[1], 'date_str': r[2]} for r in fund_core_cn_hook.get_records(error_list_sql)]
    # def check(error_list):
    if not error_list:
        logging.info('error_list没有值，不做处理')
        # 不做任何处理
        return
    error_obj = {}
    # 进行时间字符串+id进行分组，减少请求次数
    for error in error_list:
        sec_id = error['sec_id']
        date_str = error['date_str']
        if sec_id in error_obj:
            date_str_list = error_obj[sec_id]
            if date_str not in date_str_list:
                date_str_list.append(date_str)
        else:
            error_obj[sec_id] = [date_str]

    jingzhi_result_list = []
    fenhong_result_list = []
    chaifen_result_list = []

    for sec_id in error_obj:
        fund_id = sec_id.strip('.FCN')
        date_str_list = error_obj[sec_id]
        jingzhi_list, fenhong_list, chaifen_list = crawl_fund(fund_id, date_str_list)
        jingzhi_list = jingzhi_list or []
        fenhong_list = fenhong_list or []
        chaifen_list = chaifen_list or []

        def c(data_list, result_list):
            for curr_data in data_list:
                date_str = curr_data['date_str']
                row = curr_data['row']
                result_list.append({
                    'fund_id': fund_id,
                    'sec_id': sec_id,
                    'date_str': date_str,
                    'data': row
                })

        c(jingzhi_list, jingzhi_result_list)
        c(fenhong_list, fenhong_result_list)
        c(chaifen_list, chaifen_result_list)
        # 是否需要间隔一小段时间
        sleep(200)
    result_obj = {
        'jingzhi': jingzhi_result_list,
        'fenhong': fenhong_result_list,
        'chaifen': chaifen_result_list,
    }
    logging.info('error_list重新抓取结束')
    # return result_obj
    # 直接进行处理
    operate_data(error_list, result_obj, fund_core_cn_hook)


def operate_data(error_list, result_obj: dict, pg_hook: PostgresHook):
    operate_jingzhi(result_obj['jingzhi'], pg_hook)
    operate_fenhong(result_obj['fenhong'], pg_hook)
    operate_chaifen(result_obj['chaifen'], pg_hook)
    # TODO...根据error_list去更新对应的时间的基金日收益


def operate_jingzhi(jingzhi_result_list, pg_hook: PostgresHook):
    normal_cache = []
    currency_cache = []
    for jingzhi_result in jingzhi_result_list:
        fund_id = jingzhi_result['fund_id']
        sec_id = jingzhi_result['sec_id']
        date_str = jingzhi_result['date_str']
        res = clean_jingzhi(jingzhi_result['data'])
        if not res:
            continue
        if res['div_split']:
            update_split_ratio(res['sec_id'], res['date'], res['div_split'], pg_hook)
        tp = res['rtn_type']
        if tp == "每万份收益":
            currency_cache.append(res)
        else:
            normal_cache.append(res)

    if normal_cache:
        upsert_nav(normal_cache, pg_hook)
        logging.debug("更新普通收益数据")

    if currency_cache:
        upsert_currency_rtn(currency_cache, pg_hook)
        logging.debug("更新每万份收益数据")


def operate_fenhong(data_list, pg_hook):
    normal_cache = []
    for data in data_list:
        fund_id = data_list['fund_id']
        sec_id = data_list['sec_id']
        date_str = data_list['date_str']
        res = _clean_dividend(data['data'], fund_id)
        if res:
            normal_cache.append(res)
    _upsert_dividend(normal_cache, pg_hook)


def operate_chaifen(data_list, pg_hook):
    normal_cache = []
    for data in data_list:
        fund_id = data_list['fund_id']
        sec_id = data_list['sec_id']
        date_str = data_list['date_str']
        res = _clean_split(data['data'], fund_id)
        if res:
            normal_cache.append(res)
    _upsert_split(normal_cache, pg_hook)


def sleep(mill=1000):
    """
    自定义睡眠，防止线程关闭
    """
    time_mill = lambda: int(round(time.time() * 1000))
    start_milli = time_mill()
    while True:
        end_milli = time_mill()
        if (end_milli - start_milli) >= mill:
            break


def clean_jingzhi(data: dict):
    """
    处理净值
    """
    if 'fund_id' not in data or len(data['fund_id'].strip()) != 6:
        logging.error("基金代码错误:%s", data)
        return
    res = {
        'sec_id': data['fund_id'] + ".FCN",
        'date': dt.datetime.strptime(data['FSRQ'], '%Y-%m-%d').date(),
        'red_state': data.get('SHZT', None),
        'sub_state': data.get('SGZT', None),
        'div_split': data['FHSP'] if 'FHSP' in data and len(data['FHSP']) > 0 else None
    }

    rtn_type = data['SYType']

    if rtn_type is None:
        # 常规方式
        res['rtn_type'] = None
        res['nav'] = float(data['DWJZ'])  # 如果没有单位净值，则失败
        res['acc_nav'] = float(data['LJJZ']) if 'LJJZ' in data and len(data['LJJZ']) > 0 else None
        res['change'] = float(data['JZZZL']) if 'JZZZL' in data and len(data['JZZZL']) > 0 else None

    elif rtn_type in ["每万份收益", "每百份收益", "每百万份收益"]:
        res['rtn_type'] = "每万份收益"
        res['wf_rtn'] = float(data['DWJZ'])
        res['annl_rtn_7days'] = float(data['LJJZ']) if 'LJJZ' in data and len(data['LJJZ']) > 0 else None
    else:
        logging.info("未知的收益类型%s" % rtn_type)
        return
    return res


def update_split_ratio(sec_id, date: dt.date, text, pg_hook: PostgresHook):
    numbers = extract_number(text)
    date_str = date.strftime("%Y-%m-%d")
    sql = None
    if "折算" in text and len(numbers) == 1:
        sql = """
            update fund_split 
            set ratio = %s 
            where sec_id = %s and split_date=%s
            """

    if "派现金" in text and len(numbers) == 1:
        sql = """
            update fund_dividen
            set divi_per_share = %s
            where sec_id=%s and ex_divi_date=%s
            """
    if sql:
        pg_hook.run(sql, parameters=(numbers[0], sec_id, date_str))


def extract_div_amount(text):
    return float(text[5:-1])


def _clean_dividend(row, fund_id):
    """
    分红数据清洗
    :return:
    """
    if not row and len(row) < 5:
        return

    divi_pay_date = row[4] if len(row) >= 5 and len(row[4]) > 0 else None
    divi_text = row[3]
    divi_per_share = extract_div_amount(divi_text)
    ex_divi_date = row[2]
    reg_date = row[1]
    year = row[0][:-1]
    return {
            'sec_id': str(fund_id) + ".FCN",
            'divi_pay_date': divi_pay_date,
            'divi_text': divi_text,
            'divi_per_share': divi_per_share,
            'ex_divi_date': ex_divi_date,
            'reg_date': reg_date,
            'year': year
        }


def _clean_split(row, fund_id):
    """
    拆分数据清洗
    :return:
    """
    if not row and len(row) < 4:
        return
    split_date = row[1]
    split_type = row[2]
    split_desc = row[3]

    if split_desc not in ('暂未披露'):
        nums = split_desc.split(u':')
        if len(nums) != 2:
            logging.error('解析比例出错：%s', split_desc)
            raise Exception()
        ratio = float(nums[1]) / float(nums[0])
    else:
        ratio = None

    year = row[0][:-1]
    return {
        'sec_id': str(fund_id) + ".FCN",
        'split_date': split_date,
        'split_type': split_type,
        'ratio': ratio,
        'split_desc': split_desc,
        'year': year,
    }


def extract_number(text):
    return re.findall("[0-9\.]+", text)


def upsert_nav(records, pg_hook: PostgresHook):
    sql = """
    insert into fund_nav_raw(sec_id, date, nav, acc_nav, change, sub_state, red_state,div_split,updated_at) VALUES 
    (%(sec_id)s,%(date)s,%(nav)s,%(acc_nav)s,%(change)s,%(sub_state)s,%(red_state)s,%(div_split)s,LOCALTIMESTAMP)
    on conflict (sec_id ,date) do nothing;
    """
    # TODO...这里是不是需要添加update的sql处理
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(sql, records)
    conn.commit()
    cursor.close()
    conn.close()


def upsert_currency_rtn(records, pg_hook: PostgresHook):
    sql = """
    insert into fund_cur_rtn_raw(sec_id, date, wf_rtn, annl_rtn_7days,sub_state, red_state,div_split, updated_at)  
    values  (%(sec_id)s,%(date)s,%(wf_rtn)s,%(annl_rtn_7days)s,%(sub_state)s,%(red_state)s,%(div_split)s,LOCALTIMESTAMP)
    on conflict (sec_id ,date) do update 
    set wf_rtn = EXCLUDED.wf_rtn,
        annl_rtn_7days = EXCLUDED.annl_rtn_7days,
        sub_state= EXCLUDED.sub_state,
        red_state = EXCLUDED.red_state,
        updated_at = EXCLUDED.updated_at
    """
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(sql, records)
    conn.commit()
    cursor.close()
    conn.close()


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
    # TODO...这里需要设置更新的sql
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
    # TODO...这里需要设置更新的sql
    cursor.executemany(query, records)
    conn.commit()
    cursor.close()
    conn.close()


# 正常最多1条，怕错误再进行一层时间的判断
def catch_jingzhi(fund_id, date_str):
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'api.fund.eastmoney.com',
    }
    url_prefix = 'http://api.fund.eastmoney.com/f10/lsjz'
    r_params = {
        'pageIndex': 1,
        'pageSize': 10,
        'fundCode': fund_id,
        'startDate': date_str,
        'endDate': date_str,
    }
    params_str = '&'.join(['%s=%s' % (key, r_params[key]) for key in r_params])
    params_str = ('?' + params_str) if params_str else ''
    url = url_prefix + params_str

    headers['Referer'] = 'http://fund.eastmoney.com/f10/jjjz_%s.html' % fund_id

    response = requests.get(url, timeout=15, headers=headers)
    response.encoding = 'utf8'
    result = response.text
    code = response.status_code
    # 得到对应时间的数据
    if code != 200 or not result:
        logging.info('没有数据')
        return None

    result = demjson.decode(result)
    sy_type = result.get('Data', {}).get('SYType', '')
    data_all = result.get('Data', {}).get('LSJZList', [])
    for data in data_all:
        fs_rq = data.get('FSRQ')
        if date_str != fs_rq:
            continue
        # 说明是了
        data['SYType'] = sy_type
        data['fund_id'] = fund_id
        return data
    return None


def crawl_by_url(url):
    response = requests.get(url, timeout=15)
    response.encoding = 'utf8'
    code = response.status_code
    if code == 200:
        return response.text
    return ''


def catch_fenhong_chaifen(fund_id, date_str_list):
    url = 'http://fund.eastmoney.com/f10/fhsp_%s.html' % fund_id
    result = crawl_by_url(url)
    if not result:
        return None, None

    response = Selector(text=result)
    fhsp_detail_tr_list = response.xpath('//table[contains(@class, "cfxq")]//tr')
    # 拆分详情
    cfxq_detail_tr_list = response.xpath('//table[contains(@class, "fhxq")]//tr')
    # 分红送配详情数据
    fhsp_detail_tb_data = []
    for tr in fhsp_detail_tr_list:
        td_list = tr.xpath('./td | ./th')
        td_txt_list = []
        for td in td_list:
            td_txt = td.xpath('./text()').extract_first('')
            td_txt = '' if td_txt == '---' else td_txt
            td_txt_list.append(td_txt)
        fhsp_detail_tb_data.append(td_txt_list)

    # 拆分详情数据
    cfxq_detail_tb_data = []
    for tr in cfxq_detail_tr_list:
        td_list = tr.xpath('./td | ./th')
        td_txt_list = []
        for td in td_list:
            td_txt = td.xpath('./text()').extract_first('')
            td_txt = '' if td_txt == '---' else td_txt
            td_txt_list.append(td_txt)
        cfxq_detail_tb_data.append(td_txt_list)

    fenhong_list = []
    if len(fhsp_detail_tb_data) > 1:
        # 如果大于2行
        title_list = fhsp_detail_tb_data[0]
        data_list = fhsp_detail_tb_data[1:]
        if data_list and len(data_list[0]) > 1:
            for row in data_list:
                if len(row) < 5:
                    continue
                # 除息日
                if row[2] not in date_str_list:
                    continue
                for curr_date_str in date_str_list:
                    if row[4] == curr_date_str:
                        fenhong_list.append({
                            'date_str': curr_date_str,
                            'row': row
                        })

    chaifen_list = []
    if len(cfxq_detail_tb_data) > 1:
        # 如果大于2行
        title_list = cfxq_detail_tb_data[0]
        data_list = cfxq_detail_tb_data[1:]
        if data_list and len(data_list[0]) > 1:
            for row in data_list:
                if len(row) < 4:
                    continue
                # 拆分折算日
                if row[1] not in date_str_list:
                    continue
                for curr_date_str in date_str_list:
                    if row[1] == curr_date_str:
                        chaifen_list.append({
                            'date_str': curr_date_str,
                            'row': row
                        })
    return fenhong_list, chaifen_list


def crawl_fund(fund_id, date_str_list):
    # 重新抓取一遍这个基金的基金净值，分红，拆分
    jingzhi_list = []
    logging.info('fund_id:%s 开始抓取净值' % fund_id)
    for date_str in date_str_list:
        jingzhi = catch_jingzhi(fund_id, date_str)
        jingzhi and jingzhi_list.append({
            'date_str': date_str,
            'row': jingzhi
        })
        sleep(100)
    logging.info('fund_id:%s 开始抓取分红拆分' % fund_id)
    fenhong_list, chaifen_list = catch_fenhong_chaifen(fund_id, date_str_list)
    # chaifen_list, fenhong_list = [{date_str, row}]
    # jingzhi_list = [{date_str, row}]
    return jingzhi_list, fenhong_list, chaifen_list


if __name__ == '__main__':
    # update_one_fund('000001', '000001', '2016-12-27')
    # sleep(1000)
    error_list = [
        {
            'id': 1,
            'sec_id': '000001.FCN',
            'date_str': '2014-11-21'
        },
        {
            'id': 2,
            'sec_id': '000001.FCN',
            'date_str': '2016-12-27'
        },
        {
            'id': 3,
            'sec_id': '000011.FCN',
            'date_str': '2018-07-02'
        },
        {
            'id': 4,
            'sec_id': '000011.FCN',
            'date_str': '2018-07-06'
        },
        {
            'id': 5,
            'sec_id': '000075.FCN',
            'date_str': '2012-02-12'
        }
    ]
    # check(error_list)
    a = '2012-02-12'
    print(a.split('-')[0])
