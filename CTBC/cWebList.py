# -*- coding: utf-8 -*-
import os
import requests
import sys
import json
import logging
import time
import ProxyGetter, Config
import random

from pyquery import PyQuery as pyq
from MagicGoogle.config import LOGGER
from datetime import datetime
if sys.version_info[0] > 2:
    from urllib.parse import quote_plus, urlparse, parse_qs
else:
    from urllib import quote_plus
    from urlparse import urlparse, parse_qs
from CBD_project.settings import BASE_DIR
from selenium import webdriver

class Baidu(object):
    logging.basicConfig()
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    def __init__(self):
        self.url = 'https://www.baidu.com/s'
        # self.proxy = ProxyGetter.GatherProxy('baidu').getproxy()

    def search(self, query, num, start, cookie):
        log = logging.getLogger('百度網頁')
        params = {
            "wd": query,
            "pn": start,
            "op": query,
            "tn": "baidutop10",
            "ie": "utf-8",
            "rsv_idx": "2",
            "rsv_pq": "9c4000e6000081ec",
            "rsv_t": "077fEbgATiq0Cg6MgmU8/M3UF1FLTT/LAABoz6vcnp7mQKdVDvHh8czESJC3zbp2Ww",
        }
        header = {
            "User-Agent": random.choice(Config.UserAgent_List),
            "Cookie": cookie
        }
        # _proxy = ProxyGetter.GatherProxy('baidu').getproxy()
        _rq = requests.Session()
        _rq.trust_env = False
        __res = _rq.get(self.url, headers=header, params=params, timeout=30)
        if '百度安全验证' in __res.content:
            yield 'system verify'
        __doc = pyq(__res.text)
        log.info(__res.url)
        for i in __doc('.result.c-container').items():
            result = {}
            result['title'] = i('.t > a').text()
            href = i('.t > a').attr('href')
            if href:
                url = self.filter_link(href)
                try:
                    __rq = requests.Session()
                    __rq.trust_env = False
                    result['url'] = __rq.head(url).headers.get('Location')
                except:
                    result['url'] = url
            text = i('.result.c-container').text()
            result['text'] = text
            yield result

    def filter_link(self, link):
        """
        Returns None if the link doesn't yield a valid result.
        Token from https://github.com/MarioVilas/google
        :return: a valid result
        """
        log = logging.getLogger('百度網頁')
        try:
            # Valid results are absolute URLs not pointing to a Google domain
            # like images.google.com or googleusercontent.com
            o = urlparse(link, 'http')
            if o.netloc:
                return link
            # Decode hidden URLs.
            if link.startswith('/url?'):
                link = parse_qs(o.query)['q'][0]
                # Valid results are absolute URLs not pointing to a Google domain
                # like images.google.com or googleusercontent.com
                o = urlparse(link, 'http')
                if o.netloc:
                    return link
        # Otherwise, or on error, return None.
        except Exception as e:
            log.exception(e)
            return None

class Tianya(object):

    def __init__(self):
        self.url = 'http://search.tianya.cn/bbs'
        # self.header = {
        #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        # }

    def search(self, query, num, start, cookie):
        log = logging.getLogger('天涯論壇')
        page = start/10 + 1
        params = {
            "q": query,
            "pn": page
        }
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
            "Cookie": cookie
        }
        __res = requests.get(self.url, headers=header, params=params)
        __res.raise_for_status()
        __doc = pyq(__res.text)
        if u'没有找到含有' in __res.text:
            print "Got nothing and retry."
            for _ in range(5):
                __res = requests.get(self.url, headers=header, params=params)
                __res.raise_for_status()
                __doc = pyq(__res.text)
                time.sleep(5)
                if u'没有找到含有' not in __res.text:
                    break
                else:
                    continue
            if u'没有找到含有' in __res.text:
                raise Exception("check this page.")
        log.info(__res.url)
        for i in __doc('div.searchListOne > ul > li').items():
            result = {}
            result['title'] = i('div > h3 > a').text()
            href = i('div > h3 > a').attr('href')
            if href:
                url = self.filter_link(href)
                result['url'] = url
            text = i('div > p').text()
            result['text'] = text
            result['published_date'] = i('p.source > span').eq(0).text()
            yield result

    def filter_link(self, link):
        """
        Returns None if the link doesn't yield a valid result.
        Token from https://github.com/MarioVilas/google
        :return: a valid result
        """
        log = logging.getLogger('天涯論壇')
        try:
            # Valid results are absolute URLs not pointing to a Google domain
            # like images.google.com or googleusercontent.com
            o = urlparse(link, 'http')
            if o.netloc:
                return link
            # Decode hidden URLs.
            if link.startswith('/url?'):
                link = parse_qs(o.query)['q'][0]
                # Valid results are absolute URLs not pointing to a Google domain
                # like images.google.com or googleusercontent.com
                o = urlparse(link, 'http')
                if o.netloc:
                    return link
        # Otherwise, or on error, return None.
        except Exception as e:
            log.exception(e)
            return None

class Xueqiu(object):

    def __init__(self):
        self.home = 'https://xueqiu.com/'
        self.url = 'https://xueqiu.com/statuses/search.json'
        self.header = {
            # "Cookie": "device_id=bee65252c885e88f6241242c21ca2132; _ga=GA1.2.319765303.1528079949; s=f519qs6hli; aliyungf_tc=AQAAANqAgmoq6QEA6PEPZXAIzmAwnIQO; xq_a_token=019174f18bf425d22c8e965e48243d9fcfbd2cc0; xq_a_token.sig=_pB0kKy3fV9fvtvkOzxduQTrp7E; xq_r_token=2d465aa5d312fbe8d88b4e7de81e1e915de7989a; xq_r_token.sig=lOCElS5ycgbih9P-Ny3cohQ-FSA; Hm_lvt_1db88642e346389874251b5a1eded6e3=1527848553,1528187257,1528265470,1528339956; u=101528339955847; __utma=1.319765303.1528079949.1529568684.1529568684.1; __utmc=1; __utmz=1.1529568684.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _gid=GA1.2.649286612.1529568688; _gat_gtag_UA_16079156_4=1; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1529573047",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }
    def get_cookie(self):
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        browser = webdriver.Chrome(os.path.join(BASE_DIR, "chromedriver.exe"), chrome_options=option)
        browser.get(self.home)
        _cookies = browser.get_cookies()
        browser.quit()
        return _cookies


    def search(self, query, num, start):
        log = logging.getLogger('雪球')
        page = start/10 + 1
        params = {
            "sort": "relevance",
            "source": "all",
            "q": query,
            "count": 10,
            "page": page,
        }
        rq = requests.Session()
        __cookie = self.get_cookie()
        for _c in __cookie:
            rq.cookies.set(_c['name'], _c['value'])
        __res = rq.get(self.url, headers=self.header, params=params)
        __res.raise_for_status()
        _json = json.loads(__res.text)
        log.info(__res.url)
        for i in _json['list']:
            result = {}
            result['title'] = i['title']
            href = self.home + i['target']
            if href:
                url = self.filter_link(href)
                result['url'] = url
            text = i['description']
            result['text'] = text
            result['published_date'] = datetime.fromtimestamp(int(i['created_at']/1000)).strftime('%Y-%m-%d')
            yield result

    def filter_link(self, link):
        """
        Returns None if the link doesn't yield a valid result.
        Token from https://github.com/MarioVilas/google
        :return: a valid result
        """
        log = logging.getLogger('雪球')
        try:
            # Valid results are absolute URLs not pointing to a Google domain
            # like images.google.com or googleusercontent.com
            o = urlparse(link, 'http')
            if o.netloc:
                return link
            # Decode hidden URLs.
            if link.startswith('/url?'):
                link = parse_qs(o.query)['q'][0]
                # Valid results are absolute URLs not pointing to a Google domain
                # like images.google.com or googleusercontent.com
                o = urlparse(link, 'http')
                if o.netloc:
                    return link
        # Otherwise, or on error, return None.
        except Exception as e:
            log.exception(e)
            return None

class Zhidao(object):

    def __init__(self):
        self.url = 'https://zhidao.baidu.com/search'
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
            "Cookie": "BAIDUID=71D1DB6E4D626E229C11E9DBA4231EEB:FG=1;PSTM=1531981415;BIDUPSID=5450D3BB4008EA2F7A21F20EE39883A0;H_PS_PSSID=1420_25810_21107_26922_20927;IKUT=459;Hm_lvt_6859ce5aaf00fb00387e6434e4fcc925=1532657861,1534145440,1534213390,1534506384;Hm_lpvt_6859ce5aaf00fb00387e6434e4fcc925=1534768175",
        }

    def search(self, query, num, start):
        log = logging.getLogger('百度知道')
        # page = start/10 + 1
        params = {
            "word": query,
            "pn": start
        }
        __res = requests.get(self.url, headers=self.header, params=params,)
        __res.raise_for_status()
        __doc = pyq(__res.content.decode('gbk'))
        log.info(__res.url)
        for i in __doc('#wgt-list > dl.dl').items():
            result = {}
            result['title'] = i('dt.dt.mb-4.line').text()
            href = i('dt.dt.mb-4.line > a').attr('href')
            if href:
                url = self.filter_link(href)
                result['url'] = url
            text = i('dd.dd.answer').text()
            result['text'] = text
            result['published_date'] = i('dd.dd.explain.f-light > span.mr-8').eq(0).text()
            yield result

    def filter_link(self, link):
        """
        Returns None if the link doesn't yield a valid result.
        Token from https://github.com/MarioVilas/google
        :return: a valid result
        """
        log = logging.getLogger('百度知道')
        try:
            # Valid results are absolute URLs not pointing to a Google domain
            # like images.google.com or googleusercontent.com
            o = urlparse(link, 'http')
            if o.netloc:
                return link
            # Decode hidden URLs.
            if link.startswith('/url?'):
                link = parse_qs(o.query)['q'][0]
                # Valid results are absolute URLs not pointing to a Google domain
                # like images.google.com or googleusercontent.com
                o = urlparse(link, 'http')
                if o.netloc:
                    return link
        # Otherwise, or on error, return None.
        except Exception as e:
            log.exception(e)
            return None

class Tieba(object):

    def __init__(self):
        self.home = 'http://tieba.baidu.com/'
        self.url = 'http://tieba.baidu.com/f/search/res'
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }

    def search(self, query, num, start):
        log = logging.getLogger('百度貼吧')
        page = start/10 + 1
        params = {
            'qw': query,
            'pn': page,
            'sm': 2,
        }
        __res = requests.get(self.url, headers=self.header, params=params)
        __res.raise_for_status()
        __doc = pyq(__res.text)
        log.info(__res.url)
        for i in __doc('div.s_post').items():
            result = {}
            result['title'] = i('span.p_title>a.bluelink').text()
            href = self.home + i('span.p_title>a.bluelink').attr('href')
            if href:
                url = self.filter_link(href)
                result['url'] = url
            text = i('div.p_content').text()
            result['text'] = text
            result['published_date'] = i('.p_green.p_date').text()
            yield result

    def filter_link(self, link):
        """
        Returns None if the link doesn't yield a valid result.
        Token from https://github.com/MarioVilas/google
        :return: a valid result
        """
        log = logging.getLogger('百度知道')
        try:
            # Valid results are absolute URLs not pointing to a Google domain
            # like images.google.com or googleusercontent.com
            o = urlparse(link, 'http')
            if o.netloc:
                return link
            # Decode hidden URLs.
            if link.startswith('/url?'):
                link = parse_qs(o.query)['q'][0]
                # Valid results are absolute URLs not pointing to a Google domain
                # like images.google.com or googleusercontent.com
                o = urlparse(link, 'http')
                if o.netloc:
                    return link
        # Otherwise, or on error, return None.
        except Exception as e:
            log.exception(e)
            return None

if __name__ == '__main__':
    # out = list(Baidu().search(u'湖北鸿翔', start=0))
    tb = Baidu()
    import pprint
    from china_big_data.models import *
    cookies = TaskInfo.objects.filter(name='baidu').values_list('cookies', flat=True)[0]

    for i in tb.search(query='湖北鸿翔', start=0, num=10, cookie=cookies):
        pprint.pprint(i)
        print i['title']
