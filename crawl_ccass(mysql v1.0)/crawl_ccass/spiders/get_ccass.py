# -*- coding: utf-8 -*-
#-----------------------------------------------------------
# scrapy CCASS Data and save to MySQL 
# Version 1.02
# @2017-06-06
#-----------------------------------------------------------
# 1 - crawl list in period:
# scrapy crawl get_ccass -a crawl_type='1' -a period='5'
#-----------------------------------------------------------
# 2 - crawl list between start_date and end_date: 
# scrapy crawl get_ccass -a crawl_type='2' -a start_date='2017-01-01' -a end_date='2017-05-31'
#-----------------------------------------------------------
# 3 - crawl specific stockid in period: 
# scrapy crawl get_ccass -a crawl_type='3' -a stockid='00001' -a period='5'
#-----------------------------------------------------------
# 4 - crawl specific stockid between start_date and end_date: 
# scrapy crawl get_ccass -a crawl_type='4' -a stockid='00001' -a start_date='2017-01-01' -a end_date='2017-05-31'
#-----------------------------------------------------------
# 5 - re-crawl missing stockid by comparing MySQL: 
# scrapy crawl get_ccass -a crawl_type='5'
#-----------------------------------------------------------

from scrapy import Spider
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from scrapy.conf import settings

from crawl_ccass.items import BrokerInfoItem

from selenium import webdriver
from selenium.webdriver.support.ui import Select as Selenium_Selector

from bs4 import BeautifulSoup
import re
from datetime import date, timedelta
import MySQLdb

class CCASS_Selenium_Spider(Spider):
    name = 'get_ccass'
    allowed_domains = ['www.hkexnews.hk']        
    
    def start_requests(self):    
        crawl_type =  self.crawl_type
        self.existing_data = self.read_existingdata()
        self.execute_data = []
        if (crawl_type == '1'):
            period = int(self.period) 
            self.execute_data = self.structure_crawldata_1(period) 
        elif (crawl_type == '2'):
            start_date = self.start_date
            end_date = self.end_date
            self.execute_data = self.structure_crawldata_2(start_date, end_date) 
        elif (crawl_type == '3'):
            stockid = self.stockid
            period = int(self.period)           
            self.execute_data = self.structure_crawldata_3(stockid, period) 
        elif (crawl_type == '4'):
            stockid = self.stockid
            start_date = self.start_date
            end_date = self.end_date
            self.execute_data = self.structure_crawldata_4(stockid, start_date, end_date)
        elif (crawl_type == '5'):
            self.execute_data = self.structure_crawldata_5()
        else:
            self.logger.info('Wrong Parameters Input')
            pass
        
        stocklist = self.get_list_from_execute_data(self.execute_data)
        ccass_url = 'http://www.hkexnews.hk/sdw/search/searchsdw.aspx'
        sel = self.open_selenium(ccass_url)    
        for stockid in stocklist:            
            self.logger.info('start to crawl %s.hk', stockid)
            sdate = self.format_date(1)
            form_data = self.get_formdata(sel, stockid, sdate)
            yield FormRequest(url=ccass_url, formdata=form_data, callback=self.parse_ccass)

    def parse_ccass(self, response):
        # Crawling   
        n = 1
        ccass_url = response.url
        try:
            stockid = self.crawl_stockid(response)
            sel = Selector(response)
            sdates = self.get_date_from_execute_data(stockid, self.execute_data)
            for sdate in sdates:                                   
                try:
                    form_data = self.get_formdata(sel, stockid, sdate)
                    yield FormRequest(url=ccass_url, formdata=form_data, callback=self.parse_ccass_2)
                except:
                    self.logger.info('fail to crawl %s at %s', stockid, sdate)
                    break
        except:
            self.logger.info('fail to crawl the web page of the above stock')

    def parse_ccass_2(self, response):
        try:
            sdate = self.crawl_date(response)
            stockid = self.crawl_stockid(response)
            if (self.get_tuple_value(stockid, sdate) in self.existing_data):
                pass
            else:
                self.existing_data.append(self.get_tuple_value(stockid, sdate))
                broker_info = self.crawl_brokerinfo(response)
                self.logger.info('parse %s on the date %s', stockid, sdate)
                br_item = BrokerInfoItem()
                br_item['stockid'] = stockid
                br_item['sdate'] = sdate
                br_item['broker_info'] = broker_info
                return br_item
        except:
            self.logger.info('fail to parse data')

    # -------------
    # Functions
    # -------------
    def structure_crawldata_1(self, period):
        execute_data = []
        stocklist = self.read_stocklist()
        for stockid in stocklist:
            n = 1
            while (n < period):
                if self.is_sunday(n):
                    pass
                else:
                    sdate = self.format_date(n)
                    check_value = self.get_tuple_value(stockid, sdate)
                    if (check_value in self.existing_data):
                        pass
                    else:
                        execute_data.append(check_value)
                n = n + 1
        return execute_data

    def structure_crawldata_2(self, s_date, e_date):
        execute_data = []
        datelist = self.get_days_between(s_date, e_date)
        stocklist = self.read_stocklist()
        for stockid in stocklist:
            for sdate in datelist:
                if ((stockid, sdate) in self.existing_data):
                    pass
                else:
                    execute_data.append((stockid, sdate))            
        return execute_data

    def structure_crawldata_3(self, stockid, period):
        execute_data = []
        n = 1
        while (n < period):
            if self.is_sunday(n):
                pass
            else:
                sdate = self.format_date(n)
                check_value = self.get_tuple_value(stockid, sdate)
                if (check_value in self.existing_data):
                    pass
                else:
                    execute_data.append(check_value)
            n = n + 1
        return execute_data

    def structure_crawldata_4(self, stockid, s_date, e_date):
        execute_data = []
        datelist = self.get_days_between(s_date, e_date)
        for sdate in datelist:
            if ((stockid, sdate) in self.existing_data):
                pass
            else:
                execute_data.append((stockid, sdate))            
        return execute_data

    def structure_crawldata_5(self):        
        # Connect to MySQL
        conn = MySQLdb.connect( host = settings.get('MYSQL_HOST'),
                                db = settings.get('CCASS_DB'),
                                user = settings.get('MYSQL_USER'), 
                                passwd = settings.get('MYSQL_PASSWD'),
                                charset = 'utf8',
                                use_unicode = True
                                )
        cursor = conn.cursor()
        # Date
        cursor.execute('SELECT DISTINCT Date FROM broker_shares GROUP BY StockID, Date ORDER BY Date DESC')
        results = cursor.fetchall()
        datelist = []
        for result in results:
            datelist.append(result[0])
        # StockID
        cursor.execute('SELECT DISTINCT StockID FROM broker_shares GROUP BY StockID, Date ORDER BY StockID DESC')
        results = cursor.fetchall()
        stocklist = []
        for result in results:
            stocklist.append(result[0])
        conn.close()
        # Stucture Data
        execute_data = []
        for stockid in stocklist:
            for sdate in datelist:
                if ((stockid, sdate) in self.existing_data):
                    pass
                else:
                    execute_data.append((stockid, sdate))
        return execute_data

    def get_days_between(self, s_date, e_date):
        sdate=s_date.split('-')
        start_date=date(int(sdate[0]),int(sdate[1]),int(sdate[2]))
        sdate=e_date.split('-')
        end_date=date(int(sdate[0]),int(sdate[1]),int(sdate[2]))
        delta=end_date-start_date
        datelist = []
        for n in range(delta.days+1):
            if((start_date+timedelta(days=n)).weekday()<6):
                datelist.append(start_date+timedelta(days=n))
        return datelist

    def get_list_from_execute_data(self, results):
        stocklist = []
        for result in results:
            if result[0] in stocklist:
                pass
            else:
                stocklist.append(result[0])
        return stocklist

    def get_date_from_execute_data(self, stockid, results):
        sdates = []
        for result in results:
            if (stockid == result[0]):
                sdate = self.format_date_str(result[1])
                sdates.append(sdate)
        return sdates

    def read_existingdata(self):
        # Connect to MySQL
        conn = MySQLdb.connect( host = settings.get('MYSQL_HOST'),
                                db = settings.get('CCASS_DB'),
                                user = settings.get('MYSQL_USER'), 
                                passwd = settings.get('MYSQL_PASSWD'),
                                charset = 'utf8',
                                use_unicode = True
                                )
        cursor = conn.cursor()
        cursor.execute('SELECT StockID, Date FROM broker_shares GROUP BY StockID, Date')
        results = cursor.fetchall()
        conn.close()
        # Load Tuple to List
        st_data = []
        for result in results:
            st_data.append(result)
        return st_data

    def get_tuple_value(self, stockid, sdate): #stockid: '00001' sdate:'2017-06-01'
        sdate = sdate.split('-')
        tuple_value = (stockid, date(int(sdate[0]),int(sdate[1]),int(sdate[2])))
        return tuple_value

    def read_stocklist(self):
        stocklist = []
        stocklist_file = open(settings.get('STOCKLIST_FILE'), 'r')
        stocklists = stocklist_file.readlines()
        stocklist_file.close()
        for stockid in stocklists:
          stocklist.append(stockid.split(',')[0])
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
        sdate_str = self.format_date_str(sdate)
        return sdate_str 

    def format_date_str(self, sdate):
        syear = str(sdate.year)
        if (sdate.month<10):
            smonth = '0' + str(sdate.month)
        else:
            smonth = str(sdate.month)
        if (sdate.day<10):
            sday = '0' + str(sdate.day)
        else:
            sday = str(sdate.day)
        sdate_str = syear + '-' + smonth + '-' + sday
        return sdate_str 

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

    def crawl_brokerinfo(self, response):
        bs4_ccass_results = BeautifulSoup(response.body, "html.parser")
        bs4_broker_info = bs4_ccass_results.find_all('tr', {'class': ['row0','row1']})
        return bs4_broker_info