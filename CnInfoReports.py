#!/usr/bin/python

import requests
import json
import os
import time
import random
from datetime import date


class CnInfoReports:
    def __init__(
        self,
        cookies={
            "JSESSIONID": "D11313E8B5C21E6F34B496244D588F41",
            "_sp_ses.2141": "*",
            "_sp_id.2141":
            "7ccbaba6-9fc0-4141-9f20-4235125859f8.1619442550.1.1619442550.1619442550.1a8551c4-b0c0-4f5a-9190-643ad6de797e",
            "routeId": ".uc2",
        },
        headers={
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
            'Content-Type':
            'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With':
            'XMLHttpRequest',
            'Origin':
            'http://www.cninfo.com.cn',
            'Referer':
            'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index',
        },
    ):
        self.cookies = cookies
        self.headers = headers
        self.all_shsz_stock = self.get_stock_json(
            'http://www.cninfo.com.cn/new/data/szse_stock.json')
        self.all_hk_stock = self.get_stock_json(
            'http://www.cninfo.com.cn/new/data/hke_stock.json')
        self.query_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
        self.data = {
            'pageNum': 1,
            'pageSize': 30,
            'column': '',
            'tabName': 'fulltext',
            'plate': '',
            'stock': '',
            'searchkey': '',
            'secid': '',
            'category': '',
            'trade': '',
            'seDate': '',
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }

    def get_stock_json(self, url):
        stock_json_text = requests.get(url,
                                       headers=self.headers,
                                       cookies=self.cookies).text
        stockList = json.loads(stock_json_text)['stockList']
        return stockList

    def remove_invalid_stock(self, stock_list):
        valid_shsz_stock = {}
        valid_hk_stock = {}
        for stock in stock_list:
            if len(stock) == 6:
                valid_shsz_stock[stock] = None
                for each in self.all_shsz_stock:
                    if stock == each['code']:
                        valid_shsz_stock[stock] = (each['orgId'], each['zwjc'])
                        break
                if not valid_shsz_stock[stock]:
                    print(stock, '找不到，跳过')
                    continue
            elif len(stock) == 5:
                valid_hk_stock[stock] = None
                for each in self.all_hk_stock:
                    if stock == each['code']:
                        valid_hk_stock[stock] = (each['orgId'], each['zwjc'])
                        break
                if not valid_hk_stock[stock]:
                    print(stock, '找不到，跳过')
                    continue
            else:
                print(stock, '请确保代码为六位数字（A股）或五位数字（港股）')
        return valid_shsz_stock, valid_hk_stock

    def download_shsz_report(self, stock_dict, seDate):
        for code in stock_dict:
            orgId = stock_dict[code][0]
            name = stock_dict[code][1].replace('*', 'S')

            if int(code) >= 600000:
                self.data['column'] = 'sse'
            else:
                self.data['column'] = 'szse'

            self.data['stock'] = code + ',' + orgId
            self.data['seDate'] = seDate
            self.data['pageNum'] = 0
            self.data[
                'category'] = 'category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh'

            hasMore = True
            while hasMore:
                self.data['pageNum'] += 1
                res = requests.post(self.query_url,
                                    headers=self.headers,
                                    data=self.data,
                                    cookies=self.cookies)
                json_text = json.loads(res.text)
                hasMore = json_text['hasMore']
                announcements = json_text['announcements']

                if not announcements:
                    print(code, ' 未查找到报告')
                    break

                if not os.path.exists('data/' + code + '_' + name):
                    os.makedirs('data/' + code + '_' + name)

                for each in announcements:
                    secName = each['secName'].replace('*', 'S')
                    announcementTitle = each['announcementTitle'].replace(
                        ' ', '').replace('/', '-')
                    adjunctUrl = each['adjunctUrl']
                    if 'PDF' not in adjunctUrl:
                        continue
                    pdf_path = ('data/' + code + '_' + name + '/' + code +
                                '_' + secName + '_' + announcementTitle +
                                '.pdf')
                    pdf_url = 'http://static.cninfo.com.cn/' + adjunctUrl

                    if not os.path.exists(pdf_path):
                        print('正在下载：', pdf_path)
                        self.download_pdf(pdf_url, pdf_path)
                        time.sleep(random.randint(1, 4))

    def download_hk_report(self, stock_dict, seDate):
        for code in stock_dict:
            orgId = stock_dict[code][0]
            name = stock_dict[code][1]

            self.data['column'] = 'hke'
            self.data['seDate'] = seDate
            self.data['pageNum'] = 0
            self.data['stock'] = code + ',' + orgId
            self.data['category'] = ''
            hasMore = True
            while hasMore:
                self.data['pageNum'] += 1
                res = requests.post(self.query_url,
                                    headers=self.headers,
                                    data=self.data,
                                    cookies=self.cookies)
                json_text = json.loads(res.text)
                hasMore = json_text['hasMore']
                announcements = json_text['announcements']

                if not announcements:
                    print(code, ' 未查找到报告')
                    break

                if not os.path.exists('data/' + code + '_' + name):
                    os.makedirs('data/' + code + '_' + name)

                for each in announcements:
                    announcementTitle = each['announcementTitle'].replace(
                        ' ', '').replace('/', '-')
                    adjunctUrl = each['adjunctUrl']

                    if 'PDF' not in adjunctUrl:
                        continue

                    if '年度报告' in announcementTitle or '年报' in announcementTitle or '中期报告' in announcementTitle or '中报' in announcementTitle or '季度报告' in announcementTitle or '季报' in announcementTitle:
                        if len(announcementTitle) > 15:
                            print(name, announcementTitle,
                                  '名称过长疑非定期报告，如有需要请自行下载',
                                  'http://static.cninfo.com.cn/' + adjunctUrl)
                            continue
                        pdf_path = 'data/' + code + '_' + name + '/' + code + '_' + name + '_' + announcementTitle + '.pdf'
                        pdf_url = 'http://static.cninfo.com.cn/' + adjunctUrl

                        if not os.path.exists(pdf_path):
                            print('正在下载：', pdf_path)
                            self.download_pdf(pdf_url, pdf_path)
                            time.sleep(random.randint(1, 4))

    def download_pdf(self, pdf_url, pdf_path):
        res = requests.get(pdf_url, headers=self.headers, cookies=self.cookies)
        with open(pdf_path, 'wb') as file:
            file.write(res.content)

    def crawl(self, stocks, seDate='2000-01-01~' + str(date.today())):
        stock_list = stocks.replace(' ', '').split(',')
        valid_shsz_stock, valid_hk_stock = self.remove_invalid_stock(
            stock_list)
        self.download_shsz_report(valid_shsz_stock, seDate)
        self.download_hk_report(valid_hk_stock, seDate)


test = CnInfoReports()
test.crawl('600600,00700')