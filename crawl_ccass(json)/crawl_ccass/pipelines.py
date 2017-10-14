# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from bs4 import BeautifulSoup
import json

class Json_Pipeline(object):
    def open_spider(self, spider):
        self.file = open('brokerinfo.jl', 'w')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):        
        bs4_ccass_results = BeautifulSoup(item['broker_html'], "html.parser")
        for tr in bs4_ccass_results.find_all('tr', {'class': ['row0','row1']}):
            broker_info = []
            for td in tr.find_all('td'):
                broker_info.append(td.getText().strip())
            # Set ID for HKSFC
            if (broker_info[1] == 'HONG KONG SECURITIES CLEARING CO. LTD.'):
                broker_info[0] = 'SFC001'
            # Set empty ID = Name
            if (broker_info[0] == ''):
                broker_info[0] = broker_info[1]
            # remove Shares_Number's ','
            broker_info[3] = broker_info[3].replace(',','')
            # remove Shares_%'s '%'
            broker_info[4] = broker_info[3].replace('%','')
            br_data = {
                'StockID' : item['stockid'],
                'Date' : item['sdate'],
                'Broker_ID' : broker_info[0],
                'Broker_Name' : broker_info[1],
                'Shares_Number' : broker_info[3],
                'Share_Percent' : broker_info[4]
            }
            line = json.dumps(br_data) + "\n"
            self.file.write(line)
        return item