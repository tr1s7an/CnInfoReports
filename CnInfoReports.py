#!/usr/bin/env python

import httpx
import os
import time
import random
import concurrent.futures
import logging
from sys import stdout


class CnInfoReports:

    def __init__(self, max_threads=5):
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
        self.client = httpx.Client(cookies=self.cookies, timeout=None)
        self.logger = logging.getLogger('CnInfoReports')
        self.all_shsz_stock = self.get_stock_json('http://www.cninfo.com.cn/new/data/szse_stock.json')
        self.all_hk_stock = self.get_stock_json('http://www.cninfo.com.cn/new/data/hke_stock.json')
        self.query_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

        self.data = {'pageNum': 1, 'pageSize': 30, 'column': '', 'tabName': '', 'plate': '', 'stock': '', 'searchkey': '', 'secid': '', 'category': '', 'trade': '', 'seDate': '', 'sortName': '', 'sortType': '', 'isHLtitle': True}

    def get_stock_json(self, url: str) -> dict:
        self.logger.info(f'请求 {url} 中...')
        stock_json = self.client.get(url, headers=self.headers).json()
        stockList = stock_json['stockList']
        self.logger.info(f'请求成功，共有{len(stockList)}条记录')
        stockDict = {each['code']: each for each in stockList}
        return stockDict

    def remove_invalid_stock(self, market: str, stock_list: list) -> dict:
        valid_stock_info = {}
        if market == 'szse':
            for stock in stock_list:
                try:
                    this_stock = self.all_shsz_stock[stock]
                    valid_stock_info[stock] = (this_stock['orgId'], this_stock['zwjc'])
                except KeyError:
                    self.logger.warning(f'【{stock}】 证券代码无效，跳过')
                    continue
        elif market == 'hke':
            for stock in stock_list:
                try:
                    this_stock = self.all_hk_stock[stock]
                    valid_stock_info[stock] = (this_stock['orgId'], this_stock['zwjc'])
                except KeyError:
                    self.logger.warning(f'【{stock} 】 证券代码无效，跳过')
                    continue
        else:
            self.logger.warning(f'【{stock}】 请确保股票代码正确')
        return valid_stock_info

    def query_announcements_info(self, filter: dict, download_pdf=False) -> list:
        valid_stock_info = self.remove_invalid_stock(filter['market'], filter['stock'])
        client = httpx.Client(headers=self.headers, cookies=self.cookies, timeout=self.timeout)
        self.logger.info(f'开始查询报告列表')

        payload = self.data
        payload['pageNum'] = 0
        payload['column'] = filter['market']
        payload['tabName'] = filter['tabName']
        payload['plate'] = ';'.join(filter['plate'])
        payload['stock'] = ';'.join([i + ',' + valid_stock_info[i][0] for i in valid_stock_info])
        payload['category'] = ';'.join(filter['category'])
        payload['trade'] = ';'.join(filter['industry'])
        payload['searchkey'] = filter['searchkey']
        payload['seDate'] = filter['seDate']

        self.logger.info(f'初始payload为：{payload}')

        announcements_info_list = []
        hasMore = True
        while hasMore:
            payload['pageNum'] += 1
            resp = client.post(self.query_url, data=payload).json()
            hasMore = resp['hasMore']
            announcements_info_list.extend(resp['announcements'])
        self.logger.info(f'查询到{len(announcements_info_list)}条记录')
        if download_pdf:
            self.start_download_announcements_pdf(announcements_info_list)
        return announcements_info_list

    def start_download_announcements_pdf(self, announcements_info_list: list) -> None:
        #print(announcements_info_list)
        client = httpx.Client(headers=self.headers, cookies=self.cookies, timeout=self.timeout)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            future_to_url = {executor.submit(self.download_announcements_pdf, announcement, client): announcement for announcement in announcements_info_list}
            for future in concurrent.futures.as_completed(future_to_url):
                _ = future.result()

    def download_announcements_pdf(self, announcement: dict, client: httpx.Client):
        sec_code = announcement['secCode']
        sec_name = announcement['secName'].replace('*', 's')
        announcement_title = announcement['announcementTitle'].replace(' ', '').replace('/', '-')
        adjunct_type = announcement['adjunctType']
        adjunct_url = announcement['adjunctUrl']

        path_dir = f'data/{sec_code}_{sec_name}'
        if not os.path.exists(path_dir):
            os.makedirs(path_dir)
        if adjunct_type != 'PDF':
            self.logger.warning(f'【{sec_code}】 {adjunct_url} 不是PDF！')
            return
        pdf_path = f'{path_dir}/{sec_code}_{sec_name}_{announcement_title}.pdf'

        flag = 'y'
        if os.path.exists(pdf_path):
            self.logger.warning(f'发现{pdf_path}文件已存在！')
            flag = input("请确认是否覆盖(y/n)：\n")
        if flag == 'y':
            self.logger.info(f'【{sec_code}】 正在下载：{pdf_path}')
            res = client.get('http://static.cninfo.com.cn/' + adjunct_url)
            with open(pdf_path, 'wb') as file:
                file.write(res.content)
                time.sleep(random.randint(1, 4))


if __name__ == '__main__':
    #设置日志样式
    logger = logging.getLogger('CnInfoReports')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(stdout)
    sh.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(sh)

    #初始化并调用
    instance = CnInfoReports()
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
        'market': 'hke',  #港股
        'tabName': 'fulltext',
        'plate': [],
        'category': [],
        'industry': [],
        'stock': [
            '00001',
        ],
        'searchkey': '',
        'seDate': '2022-01-01~2023-02-18',
    }
    instance.query_announcements_info(filter1, download_pdf=True)
