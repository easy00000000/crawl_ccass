# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy.conf import settings
import random

class RandomUserAgentMiddleware(object):
    def process_request(self, request, spider):
        ua = random.choice(settings.get('USER_AGENT_LIST'))
        if ua:
            request.headers.setdefault('User-Agent', ua)

# class ProxyMiddleware(object):
#     def __init__(self):
#         self.proxy_list = settings.get('PROXY_FILE')
#         with open(self.proxy_list) as ip_file:
#             self.proxies = [ip.strip() for ip in ip_file]

#     def process_request(self, request, spider):
#         request.meta['proxy'] = 'http://{}'.format(random.choice(self.proxies))
#         print 'Crawling with IP ' + request.meta['proxy']