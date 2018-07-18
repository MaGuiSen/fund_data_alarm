# -*- coding: utf-8 -*-
import requests
import logging
from scrapy import Selector
import demjson
import time
# from airflow.hooks.postgres_hook import PostgresHook


def sleep(mill=1000):
    """
    自定义睡眠，防止线程关闭
    """
    time_mill = lambda: int(round(time.time() * 1000))
    start_milli = time_mill()
    print(start_milli)
    while True:
        end_milli = time_mill()
        if (end_milli - start_milli) >= mill:
            break
    print(end_milli)


def check_update_month_rtn(fund_core_cn_conn_id):
    PostgresHook = {}
    fund_core_cn_hook = PostgresHook(postgres_conn_id=fund_core_cn_conn_id)
    error_list_sql = 'select * from fund_daily_rtn WHERE abs(rtn) > 0.2'
    error_list = [{'id': r[0], 'sec_id': r[1], 'date_str': r[2]} for r in fund_core_cn_hook.get_records(error_list_sql)]
# def check(error_list):
    if not error_list:
        print('error_list没有值，不做处理')
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
        jingzhi_list, fenhong_list, chaifen_list = update_fund(fund_id, date_str_list)
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
    print('error_list重新抓取结束')
    logging.info('error_list重新抓取结束')


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


def update_fund(fund_id, date_str_list):
    # 重新抓取一遍这个基金的基金净值，分红，拆分
    jingzhi_list = []
    print('fund_id:%s 开始抓取净值' % fund_id)
    logging.info('fund_id:%s 开始抓取净值' % fund_id)
    for date_str in date_str_list:
        jingzhi = catch_jingzhi(fund_id, date_str)
        jingzhi and jingzhi_list.append({
            'date_str': date_str,
            'row': jingzhi
        })
        sleep(100)
    print('fund_id:%s 开始抓取分红拆分' % fund_id)
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