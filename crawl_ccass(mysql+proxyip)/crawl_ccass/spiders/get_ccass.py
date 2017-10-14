# -*- coding: utf-8 -*-
from scrapy import Spider
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from crawl_ccass.items import BrokerInfoItem

from selenium import webdriver
from selenium.webdriver.support.ui import Select as Selenium_Selector

import re
from datetime import date, timedelta

class CCASS_Selenium_Spider(Spider):
    name = 'get_ccass'
    allowed_domains = ['www.hkexnews.hk']        
    
    def start_requests(self):     
        self.period = int(self.period)
        ccass_url = 'http://www.hkexnews.hk/sdw/search/searchsdw.aspx'
        sel = self.open_selenium(ccass_url)
        stocklist = self.read_stocklist()
        for stockid in stocklist:            
            self.logger.info('start to crawl %s.hk', stockid)
            sdate = self.format_date(1)
            form_data = self.get_formdata(sel, stockid, sdate)
            yield FormRequest(url=ccass_url, formdata=form_data, callback=self.parse_ccass)

    def parse_ccass(self, response):
        # Crawling   
        n = 1
        ccass_url = response.url
        stockid = self.crawl_stockid(response)
        sel = Selector(response)
        while (n < self.period):                                    
            if self.is_sunday(n):
                pass
            else:
                try:
                    sdate = self.format_date(n)
                    form_data = self.get_formdata(sel, stockid, sdate)
                    yield FormRequest(url=ccass_url, formdata=form_data, callback=self.parse_ccass_2)
                except:
                    self.logger.info('fail to crawl data at %s', self.sdate)
                    break
            n = n + 1

    def parse_ccass_2(self, response):
        try:
            sdate = self.crawl_date(response)
            stockid = self.crawl_stockid(response)
            self.logger.info('parse %s on the date %s', stockid, sdate)
            br_item = BrokerInfoItem()
            br_item['stockid'] = stockid
            br_item['sdate'] = sdate
            br_item['broker_html'] = response.body
            return br_item
        except:
            self.logger.info('fail to parse data')

    # -------------
    # Functions
    # -------------
    def read_stocklist(self):
        stocklist = []
        txt_path = '/root/Spider/crawl_ccass/Base_Data/'
        txt_file = open(txt_path + 'stocklist.txt', 'r')
        stocklists = txt_file.readlines()
        txt_file.close()
        for stockid in stocklists:
          stocklist.append(stockid[:5])
        return stocklist

    def open_selenium(self, ccass_url): # Selenium to get the 1st Form Data     
        # Open Selenium 
        driver = webdriver.Firefox() #PhantomJS()
        driver.get(ccass_url)        
        # Input Initial StockID
        stockid_input = driver.find_element_by_xpath('//*[@id="txtStockCode"]')
        stockid_input.click()
        stockid_input.send_keys('00001')      
        # Click Search
        search_button = driver.find_element_by_xpath('//input[@id="btnSearch"]')
        search_button.click()
        # Load Page to Scrapy
        sel = Selector(text=driver.page_source) 
        # Close Selenium
        driver.quit()
        # Return Selector Handle
        return sel

    def format_date(self, n): # Structure Date Format 
        sdate = date.today() - timedelta(n)           
        syear = str(sdate.year)
        if (sdate.month<10):
            smonth = '0' + str(sdate.month)
        else:
            smonth = str(sdate.month)
        if (sdate.day<10):
            sday = '0' + str(sdate.day)
        else:
            sday = str(sdate.day)
        sdate = syear + '-' + smonth + '-' + sday
        return sdate 

    def is_sunday(self, n):
        sdate = date.today() - timedelta(n)
        if (sdate.weekday() < 6): # Sunday = 6 and Monday = 0 
            return False
        else:
            return True

    def get_formdata(self, sel, stockid, sdate):  # Fill Form and Return for FormRequest
        view_stat = sel.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
        view_generator = sel.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first()
        event_valid = sel.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()
        sdate = sdate.split('-')
        syear = sdate[0]
        smonth = sdate[1]
        sday = sdate[2]
        formdata = {'__VIEWSTATE': view_stat,
                    '__VIEWSTATEGENERATOR': view_generator,
                    '__EVENTVALIDATION': event_valid,
                    'today':str(date.today()).replace('-',''),
                    'sortBy':'',
                    'selPartID':'',
                    'alertMsg':'',
                    'ddlShareholdingDay':sday,
                    'ddlShareholdingMonth':smonth,
                    'ddlShareholdingYear':syear,
                    'txtStockCode':stockid,
                    'txtStockName':'',
                    'txtParticipantID':'',
                    'txtParticipantName':'',
                    'btnSearch.x':'40',
                    'btnSearch.y':'14'}
        return formdata

    def crawl_date(self, response):   # Crawl Date String from Web Page
        sdate = re.findall(r'\d{2}/\d{2}/\d{4}',Selector(response).extract())[0]     
        sdate = sdate.split('/')
        syear = sdate[2]
        smonth = sdate[1]
        sday = sdate[0]
        sdate = syear + '-' + smonth + '-' + sday
        return sdate

    def crawl_stockid(self, response):  # Crawl StockID from Web Page
        stockid = response.xpath('//span[contains(@class,"mobilezoom")]/text()').extract_first().strip()
        return stockid