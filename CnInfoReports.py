#!/usr/bin/env python

import httpx
import os
import time
import random
import concurrent.futures
import logging
from datetime import date
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0',
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
        self.data = {'pageNum': 1, 'pageSize': 30, 'column': '', 'tabName': 'fulltext', 'plate': '', 'stock': '', 'searchkey': '', 'secid': '', 'category': '', 'trade': '', 'seDate': '', 'sortName': '', 'sortType': '', 'isHLtitle': 'true'}

    def get_stock_json(self, url: str) -> dict:
        self.logger.info(f'请求 {url} 中...')
        stock_json = self.client.get(url, headers=self.headers).json()
        stockList = stock_json['stockList']
        self.logger.info(f'请求成功，共有{len(stockList)}条记录')
        stockDict = {each['code']: each for each in stockList}
        return stockDict

    def remove_invalid_stock(self, stock_list: list) -> set:
        valid_shsz_stock = []
        valid_hk_stock = []
        for stock in stock_list:
            if len(stock) == 6:
                try:
                    this_stock = self.all_shsz_stock[stock]
                    valid_shsz_stock.append((this_stock['orgId'], this_stock['zwjc'], stock))
                except KeyError:
                    self.logger.warning(f'【{stock}】 证券代码无效，跳过')
                    continue
            elif len(stock) == 5:
                try:
                    this_stock = self.all_hk_stock[stock]
                    valid_hk_stock.append((this_stock['orgId'], this_stock['zwjc'], stock))
                except KeyError:
                    self.logger.warning(f'【{stock} 】 证券代码无效，跳过')
                    continue
            else:
                self.logger.warning(f'【{stock}】 请确保代码为六位数字（A股）或五位数字（港股）')
        return valid_shsz_stock, valid_hk_stock

    def download_shsz_report(self, stock_tuple: tuple, seDate: str) -> None:
        client = httpx.Client(cookies=self.cookies, timeout=self.timeout)
        orgId, name, code = stock_tuple
        name = name.replace('*', 'S')
        payload = self.data
        self.logger.info(f'【{code}】 开始查询报告')

        payload['pageNum'] = 0
        payload['column'] = 'sse' if int(code) >= 600000 else 'szse'
        payload['stock'] = code + ',' + orgId
        payload['category'] = 'category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh'
        payload['seDate'] = seDate

        hasMore = True
        while hasMore:
            payload['pageNum'] += 1
            res = client.post(self.query_url, headers=self.headers, data=payload)
            json_text = res.json()
            hasMore = json_text['hasMore']
            announcements = json_text['announcements']

            if not announcements:
                continue

            if not os.path.exists('data/' + code + '_' + name):
                os.makedirs('data/' + code + '_' + name)

            for each in announcements:
                secName = each['secName'].replace('*', 'S')
                announcementTitle = each['announcementTitle'].replace(' ', '').replace('/', '-')
                adjunctUrl = each['adjunctUrl']
                if 'PDF' not in adjunctUrl:
                    continue
                pdf_path = ('data/' + code + '_' + name + '/' + code + '_' + secName + '_' + announcementTitle + '.pdf')
                pdf_url = 'http://static.cninfo.com.cn/' + adjunctUrl

                if not os.path.exists(pdf_path):
                    self.logger.info(f'【{code}】 正在下载：{pdf_path}')
                    res = client.get(pdf_url, headers=self.headers)
                    with open(pdf_path, 'wb') as file:
                        file.write(res.content)
                    time.sleep(random.randint(1, 4))

    def download_hk_report(self, stock_tuple: tuple, seDate: str) -> None:
        client = httpx.Client(cookies=self.cookies, timeout=self.timeout)
        orgId, name, code = stock_tuple
        payload = self.data

        payload['pageNum'] = 0
        payload['column'] = 'hke'
        payload['stock'] = code + ',' + orgId
        payload['category'] = ''
        payload['seDate'] = seDate

        hasMore = True
        while hasMore:
            payload['pageNum'] += 1
            res = client.post(self.query_url, headers=self.headers, data=payload)
            json_text = res.json()
            hasMore = json_text['hasMore']
            announcements = json_text['announcements']

            if not announcements:
                continue

            if not os.path.exists('data/' + code + '_' + name):
                os.makedirs('data/' + code + '_' + name)

            for each in announcements:
                announcementTitle = each['announcementTitle'].replace(' ', '').replace('/', '-')
                adjunctUrl = each['adjunctUrl']

                if 'PDF' not in adjunctUrl:
                    continue

                if '年度报告' in announcementTitle or '年报' in announcementTitle or '中期报告' in announcementTitle or '中报' in announcementTitle or '季度报告' in announcementTitle or '季报' in announcementTitle:
                    if len(announcementTitle) > 15:
                        self.logger.info(f'【{code} 】 名称过长疑非定期报告，如有需要请自行下载：【{name}】《{announcementTitle}》 http://static.cninfo.com.cn/{adjunctUrl}')
                        continue
                    pdf_path = 'data/' + code + '_' + name + '/' + code + '_' + name + '_' + announcementTitle + '.pdf'
                    pdf_url = 'http://static.cninfo.com.cn/' + adjunctUrl

                    if not os.path.exists(pdf_path):
                        self.logger.info(f'【{code} 】 正在下载：{pdf_path}')
                        res = client.get(pdf_url, headers=self.headers)
                        with open(pdf_path, 'wb') as file:
                            file.write(res.content)
                        time.sleep(random.randint(1, 4))

    def crawl(self, stocks: str, seDate: str = '2000-01-01~' + str(date.today())) -> None:
        stock_list = stocks.replace(' ', '').split(',')
        valid_shsz_stock, valid_hk_stock = self.remove_invalid_stock(stock_list)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            future_to_url = {executor.submit(self.download_shsz_report, stock, seDate): stock for stock in valid_shsz_stock}
            for future in concurrent.futures.as_completed(future_to_url):
                whatever = future.result()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            future_to_url = {executor.submit(self.download_hk_report, stock, seDate): stock for stock in valid_hk_stock}
            for future in concurrent.futures.as_completed(future_to_url):
                whatever = future.result()


if __name__ == '__main__':
    logger = logging.getLogger('CnInfoReports')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(stdout)
    sh.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(sh)
    myreports = CnInfoReports()
    myreports.crawl(','.join(['{:0>6d}'.format(i) for i in range(600031, 600040)]) + ',' + ','.join(['{:0>5d}'.format(i) for i in range(10, 20)]), '2020-01-01~2021-01-01')
