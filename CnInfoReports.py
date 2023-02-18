#!/usr/bin/env python

import httpx
import os
import json
import time
import random
import concurrent.futures
import logging
from sys import stdout


class CnInfoReports:

    def __init__(self, max_threads=5, skip_download_stock_json=False):
        self.cookies = {
            'JSESSIONID': '9A110350B0056BE0C4FDD8A627EF2868',
            'insert_cookie': '37836164',
            '_sp_ses.2141': '*',
            '_sp_id.2141': 'e4c90bcb-6241-49c0-b9ae-82f4b24105c3.1620969681.1.1620969681.1620969681.ad5d4abf-09e0-4b3f-95ae-e48a19f5d659',
            'routeId': '.uc1',
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'http://www.cninfo.com.cn',
            'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index',
        }
        self.max_threads = max_threads
        self.timeout = httpx.Timeout(60.0)
        self.logger = logging.getLogger('CnInfoReports')
        self.column_to_market = {'szse': 'szse', 'hke': 'hke', 'gfzr': 'third', 'fund': 'fund', 'bond': 'bond'}  #key是json的地址，value是对应的market查询参数
        if skip_download_stock_json:
            with open('stocks.json', mode='r') as file:
                data = file.read()
            self.market_to_stocks = json.loads(data)
        else:
            self.market_to_stocks = self.get_stock_json(self.column_to_market)
        self.query_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

    def get_stock_json(self, columns: dict) -> dict:
        client = httpx.Client(headers=self.headers, cookies=self.cookies, timeout=None)
        market_to_stocks = {}
        self.logger.info('开始更新代码数据')
        for column in columns:
            self.logger.info(f'请求 http://www.cninfo.com.cn/new/data/{column}_stock.json 中...')
            stock_json = client.get(f'http://www.cninfo.com.cn/new/data/{column}_stock.json').json()
            stockList = stock_json['stockList']
            self.logger.info(f'请求成功，共有{len(stockList)}条记录')
            market_to_stocks[self.column_to_market[column]] = {each['code']: each for each in stockList}
        with open('stocks.json', mode='w') as file:
            json.dump(market_to_stocks, file)
        self.logger.info('代码数据更新完毕')
        return market_to_stocks

    def remove_invalid_stock(self, market: str, stock_list: list) -> dict:
        valid_stock_info = {}
        all_stock_in_this_market = {}
        try:
            all_stock_in_this_market = self.market_to_stocks[market]
        except KeyError:
            self.logger.warning(f'请确认market【{market}】是否正确')
        for stock in stock_list:
            try:
                this_stock = all_stock_in_this_market[stock]
                valid_stock_info[stock] = this_stock['orgId']
            except KeyError:
                self.logger.warning(f'【{stock}】证券代码无效，跳过')
                continue
        return valid_stock_info

    def query_announcements_info(self, filter: dict, download_pdf=False) -> list:
        valid_stock_info = self.remove_invalid_stock(filter['market'], filter['stock'])
        client = httpx.Client(headers=self.headers, cookies=self.cookies, timeout=self.timeout)
        self.logger.info('开始查询报告列表')
        payload = {
            'pageNum': 0,
            'pageSize': 30,
            'column': filter['market'],
            'tabName': filter['tabName'],
            'plate': ';'.join(filter['plate']),
            'stock': ';'.join([i + ',' + valid_stock_info[i] for i in valid_stock_info]),
            'searchkey': filter['searchkey'],
            'secid': '',
            'category': ';'.join(filter['category']),
            'trade': ';'.join(filter['industry']),
            'seDate': filter['seDate'],
            'sortName': '',
            'sortType': '',
            'isHLtitle': False
        }
        self.logger.info(f'初始payload为：{payload}')

        announcements_info_list = []
        hasMore = True
        while hasMore:
            payload['pageNum'] += 1
            resp = client.post(self.query_url, data=payload).json()
            hasMore = resp['hasMore']
            if resp['announcements']:
                announcements_info_list.extend(resp['announcements'])
        self.logger.info(f'查询到{len(announcements_info_list)}条记录')
        if download_pdf:
            self.start_download_announcements_pdf(announcements_info_list)
        return announcements_info_list

    def start_download_announcements_pdf(self, announcements_info_list: list) -> None:
        client = httpx.Client(headers=self.headers, cookies=self.cookies, timeout=self.timeout)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            future_to_url = {
                executor.submit(self.download_announcements_pdf, announcement, client): announcement
                for announcement in announcements_info_list
            }
            for future in concurrent.futures.as_completed(future_to_url):
                _ = future.result()

    def download_announcements_pdf(self, announcement: dict, client: httpx.Client) -> None:
        sec_code = announcement['secCode']
        sec_name = announcement['secName'].replace('*', 's')
        announcement_title = announcement['announcementTitle'].replace('/', '-')
        adjunct_type = announcement['adjunctType']
        adjunct_url = announcement['adjunctUrl']
        announcement_id = announcement['announcementId']

        path_dir = f'data/{sec_code}_{sec_name}'
        if not os.path.exists(path_dir):
            os.makedirs(path_dir)
        if adjunct_type != 'PDF':
            self.logger.warning(f'【{sec_code}】{adjunct_url} 不是PDF！')
            return
        pdf_path = f'{path_dir}/{sec_code}_{sec_name}_{announcement_title}_{announcement_id}.pdf'
        if not os.path.exists(pdf_path):
            self.logger.info(f'【{sec_code}】正在下载：{pdf_path}')
            resp = client.get('http://static.cninfo.com.cn/' + adjunct_url)
            with open(pdf_path, 'wb') as file:
                file.write(resp.content)
                time.sleep(random.randint(1, 4))
        else:
            self.logger.warning(f'【{sec_code}】文件已存在，跳过下载：{pdf_path}')


if __name__ == '__main__':
    #设置日志样式
    logger = logging.getLogger('CnInfoReports')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(stdout)
    sh.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(sh)

    #初始化并调用
    instance = CnInfoReports(skip_download_stock_json=True)
    filter1 = {
        'market': 'szse',  #深沪京
        'tabName': 'fulltext',  #公告
        'plate': [],  #板块
        'category': ['category_ndbg_szsh', 'category_bndbg_szsh', 'category_sjdbg_szsh', 'category_yjdbg_szsh'],  #公告分类
        'industry': [],  #行业
        'stock': [
            '688691',
            '688690',
        ],  #股票代码
        'searchkey': '',  #标题关键字
        'seDate': '2022-01-01~2023-02-18',  #起始时间
    }
    filter2 = {
        'market': 'hke',  #港股，当前无category和industry
        'tabName': 'fulltext',
        'plate': [],
        'category': [],
        'industry': [],
        'stock': ['00001'],
        'searchkey': '',
        'seDate': '2022-01-01~2023-02-18',
    }
    instance.query_announcements_info(filter1, download_pdf=True)
