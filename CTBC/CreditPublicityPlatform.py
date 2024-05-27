# -*- coding: utf-8 -*-
import os;os.environ['DJANGO_SETTINGS_MODULE'] = 'CBD_project.settings'
import django;django.setup()
import requests
import logging
import json
from retrying import *
from china_big_data.models import *
requests.packages.urllib3.disable_warnings()
logging.getLogger("hpack").setLevel(logging.WARNING)
logging.getLogger("hyper").setLevel(logging.WARNING)
logging.getLogger("django").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)


class credit_publicity(object):

    def __init__(self):
        self.lostcredit_url = 'http://credit.customs.gov.cn/ccppserver/ccpp/queryList'
        self.abnormal_url = 'http://credit.customs.gov.cn/ccppserver//ccpp/queryListAbnormal'
        self.head = {
            "Content-Type": "application/json; charset=UTF-8",
            "Host": "credit.customs.gov.cn",
            "Origin": "http://credit.customs.gov.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
        }
        self.lostcredit_json_keys = ['socialCreditCode', 'nameSaic', 'manaType', 'depCodeChgName', 'manatypeUseTime', ]
        self.abnormal_json_keys = ['socialCreditCode', 'tradeName', 'manaType', 'customsCodeName', 'moveInDate', 'moveInReason']

    def random_rest(self, _min=5, _max=30):
        log = logging.getLogger('CreditPublish')
        _rest = random.uniform(_min, _max)
        log.info("REST TIME: %s..." % _rest)
        time.sleep(_rest)
        return

    @retry(stop='stop_after_attempt', stop_max_attempt_number=5, wait='random_sleep', wait_random_min=1000, wait_random_max=5000)
    def get_lostcredit_json(self, _curPage):
        _data = {
            "manaType": "C",
            "apanage": "",
            "depCodeChg": "",
            "curPage": _curPage,
            "pageSize": 20
        }
        try:
            _res = requests.post(self.lostcredit_url, headers=self.head, data=json.dumps(_data), verify=False)
            _json = json.loads(_res.text)
            if 'totalCount' in _json['data'].keys():
                return _json
            else:
                raise Exception('None')
        except Exception as e:
            raise Exception("Error at: %s" % e)

    @retry(stop='stop_after_attempt', stop_max_attempt_number=5, wait='random_sleep', wait_random_min=1000, wait_random_max=5000)
    def get_abnormal_json(self, _curPage):
        _data = {
            "apanage": "",
            "depCodeChg": "",
            "curPage": _curPage,
            "pageSize": 20
        }
        try:
            _res = requests.post(self.abnormal_url, headers=self.head, data=json.dumps(_data), verify=False)
            _json = json.loads(_res.text)
            if 'totalCount' in _json['data'].keys():
                return _json
            else:
                raise Exception('None')
        except Exception as e:
            raise Exception("Error at: %s" % e)

    def get_totpage(self, _json):
        _totCount = _json['data']['totalCount']
        _totPage = int(_totCount)//20+1 if int(_totCount) % 20 != 0 else int(_totCount)//20
        return _totPage

    def get_data(self, _list, _keys):
        _tmp_data = list()
        for _l in _list:
            _tmp = [_l[_k] for _k in _keys]
            _tmp_data.append(dict(zip(_keys, _tmp)))
        return _tmp_data

    def main_lostcredit(self):
        _1st_json = self.get_lostcredit_json(1)
        _tot = LostCredit.objects.all().count()
        if _tot == _1st_json['data']['totalCount']:
            return 'No update in lostcredit.'
        _pages = self.get_totpage(_1st_json)
        # _out_data = list()
        _keys = self.lostcredit_json_keys
        # ['socialCreditCode', 'nameSaic', 'manaType', 'depCodeChgName', 'manatypeUseTime', ]
        print ">>>> Lost credit review start"
        for _p in range(_pages):
            _tmp_json = self.get_lostcredit_json(_p+1)
            _tmp_data = self.get_data(_tmp_json['data']['copInfoList'], _keys)
            # _out_data += _tmp_data
            for _t in _tmp_data:
                if _t['manaType'] == "C":
                    _manaType = u'失信企业'
                elif _t['manaType'] == "B":
                    _manaType = u'一般信用企业'
                elif _t['manaType'] == "1":
                    _manaType = u'一般认证企业'
                elif _t['manaType'] == "0":
                    _manaType = u'高级认证企业'
                else:
                    _manaType = _t['manaType']
                LostCredit.objects.update_or_create(CompanyName=_t['nameSaic'],
                                                    manatypeUseTime=_t['manatypeUseTime'],
                                                    defaults={
                                                        'CreditCode': _t['socialCreditCode'],
                                                        'manaType': _manaType,
                                                        'DepCodeChgName': _t['depCodeChgName']})
            self.random_rest(_min=5, _max=10)
            print "Current page: %s" % (_p+1)
        print ">>>> Lost credit review end"
        return 'Lostcredit update complete.'

    def main_abnormal(self):
        _1st_json = self.get_abnormal_json(1)
        _tot = Abnormal.objects.all().count()
        if _tot == _1st_json['data']['totalCount']:
            return 'No update in Abnormal.'
        _pages = self.get_totpage(_1st_json)
        # _out_data = list()
        _keys = self.abnormal_json_keys
        # ['socialCreditCode', 'tradeName', 'manaType', 'customsCodeName', 'moveInDate', 'moveInReason']
        print ">>>> Abnormal review start"
        for _p in range(_pages):
            _tmp_json = self.get_abnormal_json(_p+1)
            _tmp_data = self.get_data(_tmp_json['data']['abnormalCompanyList'], _keys)
            # _out_data += _tmp_data
            for _t in _tmp_data:
                if _t['manaType'] == "C":
                    _manaType = u'失信企业'
                elif _t['manaType'] == "B":
                    _manaType = u'一般信用企业'
                elif _t['manaType'] == "1":
                    _manaType = u'一般认证企业'
                elif _t['manaType'] == "0":
                    _manaType = u'高级认证企业'
                else:
                    _manaType = _t['manaType']
                Abnormal.objects.update_or_create(CompanyName=_t['tradeName'],
                                                  moveInDate=_t['moveInDate'],
                                                  defaults={
                                                      'CreditCode': _t['socialCreditCode'],
                                                      'CompanyName': _t['tradeName'],
                                                      'manaType': _manaType,
                                                      'DepCodeChgName': _t['customsCodeName']})
            self.random_rest(_min=5, _max=10)
            print "Current page: %s" % (_p+1)
        print ">>>> Abnormal review end"
        return "Abnormal update complete."

if __name__ == '__main__':
    cp = credit_publicity()
    # cp.main_lostcredit()
    cp.main_abnormal()

if __name__ == '__main__X':
    cp = credit_publicity()
    _1st_json = cp.get_lostcredit_json(1)
    _pages = cp.get_totpage(_1st_json)
    # _out_data = list()
    _keys = cp.lostcredit_json_keys
    # ['socialCreditCode', 'nameSaic', 'manaType', 'depCodeChgName', 'manatypeUseTime', ]
    for _p in range(_pages):
        _tmp_json = cp.get_lostcredit_json(_p+1)
        _tmp_data = cp.get_data(_tmp_json['data']['copInfoList'], _keys)
        # _out_data += _tmp_data
        for _t in _tmp_data:
            if _t['manaType'] == "C":
                _manaType = u'失信企业'
            elif _t['manaType'] == "B":
                _manaType = u'一般信用企业'
            elif _t['manaType'] == "1":
                _manaType = u'一般认证企业'
            elif _t['manaType'] == "0":
                _manaType = u'高级认证企业'
            else:
                _manaType = _t['manaType']
            LostCredit.objects.create(CreditCode=_t['socialCreditCode'],
                                    CompanyName=_t['nameSaic'],
                                    manaType=_manaType,
                                    DepCodeChgName=_t['depCodeChgName'],
                                    manatypeUseTime=_t['manatypeUseTime'],
                                    )
        cp.random_rest(_min=5, _max=10)
        print "Current page: %s" % (_p+1)

if __name__ == '__main__X':
    cp = credit_publicity()
    _1st_json = cp.get_abnormal_json(1)
    _pages = cp.get_totpage(_1st_json)
    # _out_data = list()
    _keys = cp.abnormal_json_keys
    # ['socialCreditCode', 'tradeName', 'manaType', 'customsCodeName', 'moveInDate', 'moveInReason']
    for _p in range(_pages):
        _tmp_json = cp.get_abnormal_json(_p+1)
        _tmp_data = cp.get_data(_tmp_json['data']['abnormalCompanyList'], _keys)
        # _out_data += _tmp_data
        for _t in _tmp_data:
            if _t['manaType'] == "C":
                _manaType = u'失信企业'
            elif _t['manaType'] == "B":
                _manaType = u'一般信用企业'
            elif _t['manaType'] == "1":
                _manaType = u'一般认证企业'
            elif _t['manaType'] == "0":
                _manaType = u'高级认证企业'
            else:
                _manaType = _t['manaType']
            Abnormal.objects.create(CreditCode=_t['socialCreditCode'],
                                    CompanyName=_t['tradeName'],
                                    manaType=_manaType,
                                    DepCodeChgName=_t['customsCodeName'],
                                    moveInDate=_t['moveInDate'],
                                    )
        cp.random_rest(_min=5, _max=10)
        print "Current page: %s" % (_p+1)
    print "Crawler Done."
