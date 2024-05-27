# -*- coding: utf-8 -*-
import os;os.environ['DJANGO_SETTINGS_MODULE'] = 'CBD_project.settings'
import django;django.setup()
import re
import Config
from china_big_data.models import *
from http.cookies import SimpleCookie
from pyquery import PyQuery as pyq
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Selenium_qcc(object):

    def __init__(self, cp_id):
        self.cp_id = cp_id
        self.cookies = TaskInfo.objects.filter(name='qichacha').values_list('cookies', flat=True)[0].encode('utf-8')
        self.home_url = 'https://www.qcc.com/'
        self.notice_col = ['CaseReason', 'CaseNum', 'CourtDate', 'Region', 'ScheduleDate',
                          'Department', 'ChiefJudge', 'Party', 'Court',
                          'CourtDep', 'NoticeContent', ]
        self.wenshu_col = ['Num', 'CaseName', 'CaseReason', 'CaseNum', 'ExecutionIdentity', '', '', 'PublicDate', '', '']
        self.WS_rm_strs = Config.gWenshu_rm_strings

    def connect_qcc(self):
        driver = webdriver.Chrome('D:\Project\ChinaBigData_GA\chromedriver.exe')
        url = 'https://www.qcc.com/firm/%s.html' % self.cp_id
        driver.get(url)
        driver.maximize_window()
        driver.delete_all_cookies()
        # upload cookies
        cookie = SimpleCookie()
        cookie.load(self.cookies)

        for key, morsel in cookie.items():
            driver.add_cookie({'name': key, 'value': morsel.value})

        driver.get(url)
        return driver

    def select_category(self, driver):
        # defaults: 開庭公告、裁判文書 皆為 法律訴訟類別
        driver.find_element_by_xpath(u'/html/body/div[1]/div[2]/div[3]/div/div/div[1]/a/h2[contains(text(), "法律诉讼")]').click()
        return driver

    def notice_detail(self, _doc):
        _dict = dict()
        for _i, _e in enumerate(self.notice_col):
            _dict[_e] = _doc('.modal-body.scroll-content > div > table > tr > td').eq(_i*2+1).text()
            if _e == 'Party':
                for _tmp in _dict['Party'].split('\n'):
                    if u'原告' in _tmp:
                        _tmp = _tmp.split('-')[-1].strip()
                        _dict['Prosecutor'] = ', '.join(_tmp.split(u'，'))
                    else:
                        _tmp = _tmp.split('-')[-1].strip()
                        _dict['Accused'] = ', '.join(_tmp.split(u'，'))
            else:
                pass
        if 'Prosecutor' not in _dict.keys():
            _dict['Prosecutor'] = 'Please check on website.'
        return _dict

    def notice_loop(self):
        # connect qcc
        driver = self.connect_qcc()
        # select category
        driver = self.select_category(driver)
        sleep(2)
        # total count
        try:
            _cnt = driver.find_element_by_xpath('//*[@id="noticelist"]/div[2]/div[1]/span[1]/a[1]/span[2]').text
        except:
            _cnt = driver.find_element_by_xpath('//*[@id="noticelist"]/div/div[1]/span[1]').text
        _data_list = list()
        js = 'var scrollDiv = document.getElementById("noticelist").offsetTop;window.scrollTo({ top: scrollDiv + %s, behavior: "smooth"});'
        _height = 0
        driver.execute_script(js % _height)
        sleep(1)
        while True:
            try:
                # get page count
                _tr = driver.find_elements_by_xpath('//*[@id="noticelist"]/div[2]/div[2]/table/tr') if driver.find_elements_by_xpath('//*[@id="noticelist"]/div[2]/div[2]/table/tr') else driver.find_elements_by_xpath('//*[@id="noticelist"]/div/div[2]/table/tr')
                _tr = _tr[1:]
                # click for detail
                for _t in _tr:
                    _t.find_element_by_xpath('td[6]').click()
                    sleep(1)
                    try:
                        element = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@class="modal-body scroll-content"]/div/table'))
                        )
                    except Exception as e:
                        print e
                    _text = driver.page_source
                    _doc = pyq(_text)
                    _dict = self.notice_detail(_doc)
                    _data_list.append(_dict)
                    sleep(1)
                    driver.find_element_by_css_selector('div.app-nmodal.modal.fade.app-risk-detail.in > div > div > div.modal-header > a').click()
                    ActionChains(driver).move_to_element(_t).perform()
                    _height += 50
                    driver.execute_script(js % _height)
                    sleep(1)
                if len(_data_list) == int(_cnt):
                    break
                else:
                    driver.find_element_by_xpath('//*[@id="noticelist"]/div/div[2]/nav/ul/li[contains(., ">")]').click()
                sleep(2)
                _height = 0
                driver.execute_script(js % _height)
                sleep(1)
            except Exception as e:
                print e
                break
        return _data_list

    def wenshu_loop(self):
        # connect qcc
        driver = self.connect_qcc()
        # select category
        driver = self.select_category(driver)
        sleep(2)
        _top_js = 'var scrollDiv = document.getElementById("wenshulist").offsetTop;window.scrollTo({ top: scrollDiv + 400, behavior: "smooth"});'
        driver.execute_script(_top_js)
        # all case reason
        driver.find_element_by_xpath(u'//*[@id="wenshulist"]/div[4]/div[1]/div/span[contains(., "案由不限")]').click()
        _cate_list = driver.find_elements_by_xpath('//*[@id="wenshulist"]/div[4]/div[1]/div/span[2]/ul/li')
        selected_reason = [_c.text for _c in _cate_list if re.sub(u'[^\u4e00-\u9fff]+', '', _c.text) not in self.WS_rm_strs + [u'不限']]
        driver.find_element_by_xpath('//*[@id="wenshulist"]/div[4]/div[1]/div/span[2]/span').click()

        _data_list = list()
        for _s in selected_reason:
            _sub_data_list = list()
            driver.find_element_by_xpath('//*[@id="wenshulist"]/div[4]/div[1]/div/span[2]/span').click()
            case_reason = _s.split('(')[0]
            sleep(1)
            driver.find_element_by_xpath(u'//*[@id="wenshulist"]/div[4]/div[1]/div/span[2]/ul/li[contains(., "%s")]' % case_reason).click()
            sleep(0.5)
            _s_cnt = driver.find_element_by_xpath('//*[@id="wenshulist"]/div[4]/div[1]/span[1]').text
            sleep(0.5)
            print _s
            pages = 1
            while True:
                try:
                    _text = driver.page_source
                    _doc = pyq(_text)
                    for _items in _doc('#wenshulist > div.tablist > div.app-ntable > table').items():
                        _row = len(_items('tr'))-1
                        _col = len(_items('tr > th'))
                        for _r in list(range(_row)):
                            _tmp = dict()
                            js = 'var scrollDiv = document.querySelector("#wenshulist > div.tablist > div.app-ntable > table > tr:nth-child(%s)").offsetTop;window.scrollTo({ top: scrollDiv + 400, behavior: "smooth"});'
                            driver.execute_script(js % (_r+1))
                            sleep(2)
                            for _c in list(range(_col)):
                                _tmp[self.wenshu_col[_c]] = _items('tr:nth-child(%s) > td:nth-child(%s)' % (_r+2, _c+1)).text()
                                if (_c+1) == 10:
                                    driver.find_element_by_css_selector('#wenshulist > div.tablist > div.app-ntable > table > tr:nth-child(%s) > td:nth-child(10)' % (_r+2)).click()
                                    sleep(1)
                                    driver.switch_to.window(driver.window_handles[-1])
                                    _html = driver.page_source
                                    if u'<title></title>' in _html:  # exception
                                        _res_d = pyq(re.sub(u'<title></title>', '', _html))
                                    else:
                                        _res_d = pyq(_html)
                                    if _res_d('div#wsview'):
                                        _tmp['Content'] = _res_d('div#wsview').html()
                                        _tmp['ExecutionCourt'] = _res_d('.qcc_law_court').text()
                                    else:
                                        raise
                                    driver.close()
                                    driver.switch_to.window(driver.window_handles[0])
                            if _tmp and any(_tmp[_k] for _k in _tmp.keys()):  # 只要有資料即儲存:
                                _sub_data_list.append(_tmp)

                    if len(_sub_data_list) == int(_s_cnt):
                        break
                    else:
                        driver.find_element_by_xpath('//*[@id="wenshulist"]/div[4]/div[2]/nav/ul/li/a[contains(text(), ">")]').click()
                    sleep(2)
                    driver.execute_script(_top_js)
                    sleep(1)
                    pages += 1
                except Exception as e:
                    print e
                    break
            _data_list += _sub_data_list
            driver.execute_script(_top_js)
            sleep(1)
            driver.find_element_by_xpath(u'//*[@id="wenshulist"]/div[4]/div[1]/div/span[contains(., "%s")]' % case_reason).click()
            sleep(0.5)
            driver.find_element_by_xpath(u'//*[@id="wenshulist"]/div[4]/div[1]/div/span[contains(., "%s")]/ul/li/a[contains(text(), "不限")]' % case_reason).click()
            sleep(0.5)
        driver.quit()
        return _data_list

if __name__ == '__main__':
    cp_id = '043c510e18e84737d3413db6be44c26d'
    sqcc = Selenium_qcc(cp_id=cp_id)
    # _list = sqcc.notice_loop()
    _list = sqcc.wenshu_loop()
    _list
