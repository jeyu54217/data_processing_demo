# -*- coding: utf-8 -*-
import requests
import re
# import pandas as pd
import logging
import json
from datetime import datetime
from pyquery import PyQuery as pyq
from MagicGoogle import MagicGoogle
try:
    from CrawlerLib import Config, cWebList
except:
    import Config, cWebList

class ZhidaoCon(object):

    def __init__(self, url):
        self.url = url
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }
        self.a_title_opt = '.ask-title'
        self.a_con_opt = '.con-all'
        self.a_time_opt = '#ask-info:nth-child(4)'
        self.ba_com_opt = '.best-text.mb-10'
        self.ba_time_opt = '.grid-r.f-aid.pos-time.answer-time.f-pening'
        self.ba_id_opt = '.wgt-best'
        self._ajx = 'https://zhidao.baidu.com/api/comment'

    def zdComment(self, _ajx, _head, threadID, start=0, u=0, _tmp_dict={}):
        _para = {
            "method": "get_reply",
            "app": "qb",
            "thread_id": threadID,
            "start": start,
            "limit": 5
        }
        _ajx_r = requests.get(_ajx, headers=_head, params=_para)
        _json = json.loads(_ajx_r.text)
        for i in _json['data']:
            _tmp = {}
            _tmp['CommentCon'] = i['content']
            _tmp['CommentTime'] = datetime.fromtimestamp(int(i['create_time'])).strftime('%Y-%m-%d')
            _tmp_dict[str(u)] = _tmp
            u += 1
        if not int(_json['total_count']) == u:
            _start = u
            _tmp_dict = self.zdComment(_ajx, _head, threadID, start=_start, u=u, _tmp_dict=_tmp_dict)
        return _tmp_dict

    def Dict(self):
        _res = requests.get(self.url, headers=self.header, verify=False, timeout=30)
        _res.raise_for_status()
        _doc = pyq(_res.content)
        # get content
        _out_dict = {}
        # author
        author = {}
        author['AuthorTitle'] = _doc(self.a_title_opt).text()
        author['AuthorCon'] = _doc(self.a_con_opt).text()
        author['AuthorTime'] = _doc(self.a_time_opt).text()
        _out_dict['Author'] = author
        # best ans
        bestans = {}
        bestans['BestAnsCon'] = _doc(self.ba_com_opt).text()
        bestans['BestAnsTime'] = _doc(self.ba_time_opt).text()

        try:
            ansId = re.sub('[^0-9]', '', _doc(self.ba_id_opt).attr('id'))
            idx = [j for j in _doc('[type="text/javascript"]').items() if ansId in j.text()]
            threadID = re.search("threadId:\"(.*)\",h", idx[0].text()).group(1)
        except Exception as e:
            print "%s: No Best Answer" % e
            threadID = None
            pass
        if threadID:
            bestans['BestAnsComment'] = self.zdComment(self._ajx, self.header, threadID)
        else:
            bestans['BestAnsComment'] = ''
        _out_dict['BestAns'] = bestans
        otherans = {}
        u = 0
        for i in _doc('div.line.content').items():
            if i('.answer-text.line > .con') and i('.pos-time'):
                other={}
                ansId = re.sub('[^0-9]', '', i('.answer-text.line').attr('id'))
                try:
                    idx = [j for j in _doc('[type="text/javascript"]').items() if ansId in j.text()]
                    threadID = re.search("threadId:\"(.*)\",h", idx[0].text()).group(1)
                except:
                    continue
                if threadID:
                    other['OthersAnsComment'] = self.zdComment(self._ajx, self.header, threadID, _tmp_dict={})
                other['OthersAnsCon'] = i('.answer-text.line > .con').text()
                other['OthersAnsTime'] = i('.pos-time').text()
                otherans[str(u)] = other
                u += 1
        _out_dict['OthersAns'] = otherans
        return _out_dict

class TiebaCon(object):

    def __init__(self, url):
        self.url = url
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }
        self.a_title_opt = '.core_title_txt'
        self.r_list_opt = '.l_post.j_l_post.l_post_bright'
        self.r_con_opt = 'div.d_post_content_main > .p_content > cc'

    def Dict(self):
        _res = requests.get(self.url, headers=self.header, verify=False, timeout=30)
        _res.raise_for_status()
        _doc = pyq(_res.text)
        _out_dict = {}
        author = {}
        author['AuthorTitle'] = _doc(self.a_title_opt).text()
        response = {}
        n = 0
        for item in _doc(self.r_list_opt).items():
            _tmp_res = {}
            if item.attr('data-field'):
                if n == 0:
                    author['AuthorCon'] = item(self.r_con_opt).text()
                    # _info = json.loads(item.attr('data-field'))
                    author['AuthorTime'] = item('span.tail-info').eq(1).text()
                    if not author['AuthorTime']:
                        _json = json.loads(item.attr('data-field'))
                        author['AuthorTime'] = _json['content']['date']
                    _out_dict['Author'] = author
                else:
                    _tmp_res['ResCon'] = item(self.r_con_opt).text()
                    # _info = json.loads(item.attr('data-field'))
                    _tmp_res['ResTime'] = item('span.tail-info').eq(1).text()
                    if not _tmp_res['ResTime']:
                        _json = json.loads(item.attr('data-field'))
                        try:
                            _tmp_res['ResTime'] = _json['content']['date']
                        except:
                            continue
                    response[str(n-1)] = _tmp_res
                n += 1
        _out_dict['Response'] = response
        return _out_dict

class XueqiuCon(object):

    def __init__(self, url):
        self.url = url
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }
        self.ajx_url = 'https://xueqiu.com/statuses/comments.json'

    def xqComment(self, _cookie, _page=1, u=0, _tmp_dict={}):
        _ajx_p = {
            'id': self.url.split('/')[-1],
            'count': '20',
            'page': _page,
            'reply': 'true',
            'asc': 'false',
            'type': 'status',
            'split': 'true',
        }
        _ajx_r = requests.get(self.ajx_url, params=_ajx_p, headers=self.header, cookies=_cookie, verify=False, timeout=30)
        _json = json.loads(_ajx_r.text)
        if not _json['maxPage'] == '0':
            for i in range(_json['count']):
                _tmp = {}
                _tmp['ResCon'] = pyq(_json['comments'][i]['text']).text()
                _tmp['ResTime'] = _json['comments'][i]['timeBefore']
                _tmp_dict[str(u)] = _tmp
                u += 1
            if not _json['count'] == u:
                _page += 1
                _tmp_dict = self.xqComment(_cookie=_cookie, _page=_page, _tmp_dict=_tmp_dict, u=u)
        return _tmp_dict

    def Dict(self):
        _res = requests.get(self.url, headers=self.header, verify=False, timeout=30)
        _doc = pyq(_res.text)
        _out_dict = {}
        author = {}
        author['AuthorTitle'] = _doc('.article__bd__title').text()
        author['AuthorCon'] = _doc('.article__bd__detail').text()
        author['AuthorTime'] = _doc('.avatar__subtitle > a').text()
        _out_dict['Author'] = author
        _cookies = _res.cookies.get_dict()
        _out_dict['Response'] = self.xqComment(_cookie=_cookies)
        return _out_dict

class TianyaCon(object):

    def __init__(self, url):
        self.url = url
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36"
        }

    def Dict(self):
        _res = requests.get(self.url, headers=self.header, verify=False, timeout=30)
        _res.raise_for_status()
        _doc = pyq(_res.text)
        _out_dict = {}
        author = {};response={}
        author['AuthorTitle'] = _doc('.s_title').text()
        n = 0
        for _i in range(len(_doc('.bbs-content'))):
            _tmp = {}
            if n == 0:
                author['AuthorCon'] = _doc('.bbs-content').eq(_i).text()
                author['AuthorTime'] = _doc('.atl-info > span:nth-child(2)').eq(_i).text()
                _out_dict['Author'] = author
            else:
                _tmp['ResCon'] = _doc('.bbs-content').eq(_i).text()
                _tmp['ResTime'] = _doc('.atl-info > span:nth-child(2)').eq(_i).text()
                response[str(n-1)] = _tmp
            n += 1
        _out_dict['Response'] = response
        return _out_dict



if __name__ == '__main__':
    # 貼吧
    # tb = TiebaCon(url='https://tieba.baidu.com/p/2738766418?pid=42494576369&cid=0&red_tag=1942760877#42494576369')
    # _tb = tb.tbDict()
    # print _tb

    # # 天涯
    # ty = TianyaCon(url='http://bbs.tianya.cn/post-828-310061-1.shtml')
    # _ty = ty.Dict()
    # print _ty

    # 知道
    zd = ZhidaoCon(url='https://zhidao.baidu.com/question/1964609167950531060.html?entry=home_new_content')
    _zd = zd.Dict()
    print _zd

    # 雪球
    # xq = XueqiuCon(url='https://xueqiu.com/1885917693/102395670')
    # _xq = xq.xqDict()
    # print _xq