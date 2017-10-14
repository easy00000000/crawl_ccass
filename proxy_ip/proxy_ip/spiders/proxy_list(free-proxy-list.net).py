# -*- coding: utf-8 -*-
from scrapy import Spider
import re

class ProxyListSpider(Spider):
    name = 'proxy_list'
    allowed_domains = ["free-proxy-list.net"]
    start_urls = ['http://free-proxy-list.net']

    def parse(self, response):
        proxy_infos = response.xpath('//tbody/tr').extract()
        ip_file = open('proxy_ip_list.txt', 'w')
        for proxy_info in proxy_infos:
        	ip_text = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',proxy_info)
        	port_text = re.findall(r'>\d{1,6}<',proxy_info)
        	port_text = re.findall(r'\d{1,6}',port_text[0])
        	yield{
        		'Proxy_IP': ip_text[0],
        		'Port': port_text[0]
        	}
        	ip_line = ip_text[0] + ':' + port_text[0] + '\n'
        	ip_file.write(ip_line)
    	ip_file.close()