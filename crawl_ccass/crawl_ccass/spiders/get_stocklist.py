# -*- coding: utf-8 -*-
#
# Crawl StockList from website http://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdeqty_pf.htm
# And save to file name stocklist.txt
# Usage: scrapy crawl get_stocklist
#

from scrapy import Spider
from scrapy.http import Request
from scrapy.conf import settings
from bs4 import BeautifulSoup

class CCASS_Selenium_Spider(Spider):
    name = 'get_stocklist'
    allowed_domains = ['www.hkex.com.hk']  

    def start_requests(self):  
        stocklist_mainboard_url = 'http://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdeqty_pf.htm'
        stocklist_gem_url = 'http://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdgems_pf.htm'
        yield Request(url=stocklist_mainboard_url, callback=self.parse_mainboard)
        yield Request(url=stocklist_gem_url, callback=self.parse_gem)

    def parse_mainboard(self, response):
        self.stocklist_mainboard = self.parse_stockinfo(response)

    def parse_gem(self, response):
        self.stocklist_gem = self.parse_stockinfo(response)  

    def closed(self, reason):        
        # Write to TXT File        
        with open(settings.get('STOCKLIST_FILE'), 'w') as txtfile:
            for row in self.stocklist_mainboard+self.stocklist_gem:
                txtfile.write(row[0]+','+row[1]+'\n')

    def parse_stockinfo(self, response):
        get_list = []
        bs4_results = BeautifulSoup(response.body, "html.parser")
        for tr in bs4_results.find_all('tr', {'class': ['tr_normal']}):
            get_stock_info = []
            for td in tr.find_all('td'):                 
                get_stock_info.append(td.getText().strip())
            get_list.append([get_stock_info[0],get_stock_info[1]])  

        return get_list
