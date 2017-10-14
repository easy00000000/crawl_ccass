# -*- coding: utf-8 -*-
from scrapy import Spider
import re
from time import sleep

class ProxyListSpider(Spider):
    name = 'proxy_list'
    allowed_domains = ['proxynova.com']
    # start_urls = ['https://www.proxynova.com/proxy-server-list/elite-proxies/'] 
    start_urls = ['https://www.proxynova.com/proxy-server-list/country-cn/'] 
    # start_urls = ['https://www.proxynova.com/proxy-server-list/country-hk/']

    def parse(self, response):
        step_minutes = 5
        while True :
            ip_lines = ''
            proxy_infos = response.xpath('//tr').extract()        
            for proxy_info in proxy_infos:
                is_elite = re.findall(r'elite',proxy_info)
                if (is_elite != []):     
                    port_text = re.findall(r'>\d{1,6}<',proxy_info)
                    if (port_text != []):
                        port_text = re.findall(r'\d{1,6}',port_text[0])[0]
                        ip_text_1 = re.findall(r'\d{4,5}\.\d{1,3}',proxy_info)
                        ip_text_2 = re.findall(r'\d{0,3}\.\d{1,3}\.\d{1,3}',proxy_info)
                        if (ip_text_1 != []):
                            if (ip_text_2 != []):
                                ip_text = ip_text_1[0] + ip_text_2[0]
                                ip_text = ip_text[2:]                
                                yield{
                                    'Proxy_IP': ip_text[0],
                                    'Port': port_text[0]
                                }
                                ip_line = ip_text + ':' + port_text
                                self.logger.info(ip_line)
                                ip_lines = ip_lines + ip_line + '\n'                                
            ip_file = open('proxy_ip_list.txt', 'w')            
            ip_file.write(ip_lines)
            ip_file.close()
            self.logger.info('Waiting ...')
            sleep(step_minutes*60)