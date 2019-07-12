#!/usr/bin/python
# -*- coding: UTF-8 -*-

import traceback as tb
import sys
reload(sys) 
sys.setdefaultencoding('utf8')
import os
import re
import time
import ConfigParser
import codecs
import requests
requests.packages.urllib3.disable_warnings()
import keyword_script as keyword

''' 今日头条 模块 '''
class toutiao():

    # 所需字段
    colDictList = [("ad_id","广告计划id"), ("ad_name","广告计划名称"), ("campaign_id","广告组id"), ("campaign_name","广告组名称"), ("stat_datetime","数据起始时间"), ("show","展示"), ("click","点击"), ("cost","总花费")]
    # 脚本所在目录
    scriptPath = os.path.split(os.path.realpath(__file__))[0]
    
    ''' 初始化 '''
    def __init__(self):
        print "===>启动 toutiao模块"
        cf = ConfigParser.ConfigParser()
        cf.read("cfg.properties")
        # 子账户
        self.subAccList = cf.get("toutiao-api", "sub_acc_list").strip().split(",")
        # 子账户:媒介
        utmMediumDict = {}
        for data in cf.get("toutiao-api", "utm_medium").strip().split(","):
            (key, value) = data.split(":")
            utmMediumDict[key] = value
        self.utmMediumDict = utmMediumDict
        # serverUrl
        self.openApiUrlPrefix = cf.get("toutiao-api", "serverUrl").strip()
        # app_id(int)
        self.appId = int(cf.get("toutiao-api", "app_id").strip())
        # secret(str)
        self.secret = cf.get("toutiao-api", "secret").strip()
        # refresh_token(str)
        self.refreshToken = cf.get("toutiao-api", "refresh_token").strip()

        
    ''' 刷新access_token, 通过此方法刷新access_token, execute()之前先调用此方法, 获取access_token '''
    def refresh_access_token(self):
        print "===> toutiao, 刷新access_token"
        try:
            url = self.openApiUrlPrefix + "oauth2/refresh_token/"
            data = {
                "appid": self.appId,
                "secret": self.secret,
                "grant_type": "refresh_token",
                "refresh_token": self.refreshToken,
            }
            requests.packages.urllib3.disable_warnings()
            rsp = requests.post(url, json=data, verify=False)
            rsp_data = rsp.json()
            if rsp_data["code"] == 0:
                accessToken = rsp_data["data"]["access_token"]
                refreshToken = rsp_data["data"]["refresh_token"]
                self.accessToken = accessToken
                save_refresh_token(refreshToken)
            else:
                raise RuntimeError(rsp_data["message"])
        except Exception, e:
            tb.print_exc()
            return (1, "func: refresh_access_token() fail!")


    ''' 广告计划数据（新版） '''
    def get_campaign_stat(self, advertiserId, batchDate, downloadPath):
        url = self.openApiUrlPrefix + "2/report/ad/get/"
        params = {
            "advertiser_id": int(advertiserId),
            "start_date": batchDate,
            "end_date": batchDate,
            "time_granularity": "STAT_TIME_GRANULARITY_DAILY",
            "group_by": ["STAT_GROUP_BY_FIELD_ID"],
            "page": 1,
            "page_size" : 1000
        }
        headers = {"Access-Token": self.accessToken}
        requests.packages.urllib3.disable_warnings()
        rsp = requests.get(url, json=params, headers=headers, verify=False)
        rsp_data = rsp.json()
        if rsp_data["code"] != 0:
            raise RuntimeError(rsp_data["message"])
        if rsp_data["data"]["page_info"]["total_number"] > 1000: # 判断总记录数>1000
            raise RuntimeError("total_number>1000, 单页无法保存所有数据, 需要特殊处理")
        jsonList = rsp_data["data"]["list"]
        sourceFile = downloadPath + str(advertiserId) + ".csv.bk"
        targetFile = sourceFile[:-3];
        print "===> targetFile is %s" % targetFile
        with open(sourceFile, "wb") as output:
            line = ""
            # 内容header
            for colDict in self.colDictList:
                line += colDict[1] + ","
            output.write(line[:-1] + "\n")
            # 内容body
            for json in jsonList:
                line = ""
                for colDict in self.colDictList:
                    if colDict[0] == "stat_datetime":
                        line += batchDate + ","
                    else:
                        line += str(json[colDict[0]]) + ","
                output.write(line[:-1] + "\n")
        # source -> target
        with open(targetFile, "wb") as output:
            # 广告计划id,广告计划名称,广告组id,广告组名称,数据起始时间,展示,点击,总花费(2,3,4,5,0,9,10,8)
            # 日期  账户  推广计划ID  推广计划  推广组ID  推广组  关键词id  关键词  消耗  展示数  点击数  点击均价  点击率  关键词排名
            with codecs.open(sourceFile, 'r', 'utf-8') as fileHandler:
                index = 0
                for line in fileHandler.readlines():
                    index += 1
                    if index == 1: # 剔除第一行
                        continue
                    columns = line.encode("utf-8").strip("\r\n").split(",")
                    ctr, cpc = None, None # 点击率, 点击均价
                    if columns[6] == "0": # 剔除"点击"为0的记录数
                        continue
                    else: # 计算点击率
                        impression, click, cost = int(columns[5]), float(columns[6]), float(columns[7]) # 展示数, 点击, 花费
                        ctr = format(click*100/impression, '.2f')
                        cpc = format(cost/click, '.2f')
                    newColumns = [columns[4], advertiserId, columns[0], columns[1], columns[2], columns[3], "", "", columns[7], columns[5], columns[6], ctr, cpc, ""]
                    newColumns.append("Feeds")
                    newColumns.append(self.utmMediumDict[advertiserId]) # utm_medium
                    newColumns.append(keyword.getCity(columns[1])) # 城市
                    output.write("\t".join(newColumns) + "\n")


    ''' 执行主程序 '''
    def execute(self, batchDate, downloadPath):
        try:
            if downloadPath[-1:] != "/":
                downloadPath += "/"
            self.refresh_access_token()
            print "===>执行 toutiao.execute(), batchDate %s" % batchDate
            # 删除文件
            strCmd = "rm -f %s*.csv*" % downloadPath
            print strCmd
            os.system(strCmd)
            for acc in self.subAccList:
                print "===>执行账号: " + acc
                self.get_campaign_stat(acc, batchDate, downloadPath)
            print "===>执行结果[Succ]"
            return 0
        except Exception, e:
            tb.print_exc()
            print "===>执行异常[Fail]"
            return 1


''' 获取 refresh_token, 并持久化到cfg.properties中, 此方法同回调url调用 '''
def get_refresh_token(authCode):
    try:
        print "===>根据auth_code, 获取refresh_code"
        cf = ConfigParser.ConfigParser()
        cf.read(toutiao.scriptPath + "/cfg.properties")
        # serverUrl
        openApiUrlPrefix = cf.get("toutiao-api", "serverUrl").strip()
        # app_id(int)
        appId = int(cf.get("toutiao-api", "app_id").strip())
        # secret(str)
        secret = cf.get("toutiao-api", "secret").strip()
        url = openApiUrlPrefix + "oauth2/access_token/"
        data = {
            "appid": appId,
            "secret": secret,
            "grant_type": "auth_code",
            "auth_code": authCode
        }
        requests.packages.urllib3.disable_warnings()
        rsp = requests.post(url, json=data, verify=False)
        rsp_data = rsp.json()
        if rsp_data["code"] == 0:
            refreshToken = rsp_data["data"]["refresh_token"]
            save_refresh_token(refreshToken)
        else:
            raise RuntimeError("获取refresh_code失败, 异常内容:" + rsp_data["message"])
    except Exception, b:
        tb.print_exc()
        raise RuntimeError("func: get_access_and_refresh_token() fail!")

            
''' 持久化 refresh_token, 通常用于回调url中的action '''
def save_refresh_token(newRefreshToken):
    print "===>保存refresh_token值:%s" % newRefreshToken
    strCmd = "sed -i 's/refresh_token=.*/refresh_token=%s/g' %s/cfg.properties"  %(newRefreshToken, toutiao.scriptPath)
    print strCmd
    os.system(strCmd)
        

''' Main函数参数校验 '''
def usage_help():
    errMsg = "使用方法:\n"
    errMsg += "  刷新token: python toutiao.py refresh_token authCode\n"
    errMsg += "  抽取数据: python toutiao.py execute yyyy-mm-dd [downloadPath]\n"
    
    if len(sys.argv) == 1 or sys.argv[1] not in ["refresh_token", "execute"]:
        print errMsg
        sys.exit(1)
    # 方法, 校验参数
    if sys.argv[1] == "refresh_token": # 刷新token
        if len(sys.argv) != 3:
            print "方法:refresh_token(), 缺少:actuCode"
            sys.exit(1)
        else:
            get_refresh_token(sys.argv[2])
    elif sys.argv[1] == "execute": # 执行函数
        if len(sys.argv) <= 2:
            print "方法:execute(), 缺少:yyyy-mm-dd, [downloadPath]"
            sys.exit(1)
        else:
            downloadPath = toutiao.scriptPath + "/download/toutiao"
            if len(sys.argv) >= 4:
                downloadPath = sys.argv[3]
            batchDate = sys.argv[2]
            toutiao().execute(batchDate, downloadPath)
    

if __name__ == "__main__":
    try:
        usage_help()
        ''' callbackUrl 调用 '''
        # authCode = "a60cc29191f49319a33be55fafeab5abeba22c87"
        # get_refresh_token(authCode)
        
        ''' execute() 调用'''
        # toutiao = toutiao()
        # batchDate = "2019-07-01"
        # downloadPath = toutiao.scriptPath + "/download/toutiao/"
        # toutiao.execute(batchDate, downloadPath)
    except Exception, e:
        tb.print_exc()
        sys.exit(1)
    


