#!/usr/bin/python
# -*- coding: UTF-8 -*-

import traceback as tb
import requests
requests.packages.urllib3.disable_warnings()
import json
import sys
import os
import time
import ConfigParser
import urllib
import codecs
import keyword_script as keyword
''' baidu sdk '''
sys.path.append('./sdk/baidu')
from ApiSDKJsonClient import *
from sms_service_ReportService import *


''' baidu类 '''
class baidu():
    
    def __init__(self):
        print "===>启动 baidu模块"
        cf = ConfigParser.ConfigParser()
        cf.read("cfg.properties")
        sougouCfg = {}
        # acc1,acc2
        self.subAccList = cf.get("baidu-api", "sub_acc_list").strip().split(",")
        self.service = sms_service_ReportService()


    #  request func(batchDate:yyyy-mm-dd)
    def execute(self, batchDate, downloadPath):
        if downloadPath[-1:] != "/":
            downloadPath += "/"
        strCmd = "rm -f %s*.csv*" % downloadPath
        print strCmd
        os.system(strCmd)
        self.startDate = batchDate + " 00:00:00"
        self.endDate = batchDate + " 23:59:59"
        failAccList = [] # 错误账户列表
        for acc in self.subAccList:
            print "===> baidu acc %s" % acc
            sendCnt = 0
            while True:
                status = self.request(acc, downloadPath)
                sendCnt += 1
                if status == 0: # Succ
                    break
                if sendCnt <= 3 : # Fail<3次
                    print "===>acc: %s, sleep and try %s time" % (acc, sendCnt)
                    time.sleep(3)
                else:
                    print "===>acc: %s, still failed, skip..." % acc
                    failAccList.append(acc)
                    break
        if len(failAccList) == 0:
            print "===>执行结果[Succ]"
            return 0
        else:
            print "===>执行异常[Fail], 错误账户列表: " + ",".join(failAccList)
            return 1


    ''' baidu 报送 '''
    def request(self, acc, downloadPath):
        response = None
        try:
            ApiSDKJsonClient.targetconf = acc
            for device in [1,2]:
                # getProfessionalReportId():异步报告
                body = {"reportRequestType":{"performanceData":["position","click","cost","ctr","impression","cpc"],"levelOfDetails":11,"startDate": ("%s " %self.startDate), "endDate":("%s " % self.endDate),"unitOfTime":5,"reportType":14,"statRange":2,"device":device,"platform":0}}
                response = self.service.getProfessionalReportId(body)
                reportId = response["body"]['data'][0]['reportId']
                time.sleep(5)
                
                # getReportState():查看报告状态
                response = self.service.getReportState({"reportId":reportId})
                isGenerated = response["body"]["data"][0]["isGenerated"]
                while isGenerated in [1,2]:
                    print "等待中, 睡眠.... "
                    time.sleep(5)
                    response = self.service.getReportState({"reportId":reportId})
                    isGenerated = response["body"]["data"][0]["isGenerated"]
               
                # getReportFileUrl():下载文件
                reportFilePath = None # request.url地址
                if response["header"]["desc"] == "success" and response["header"]["status"] == 0:
                    response = self.service.getReportFileUrl({"reportId":reportId})
                    reportFilePath = response["body"]["data"][0]["reportFilePath"]
                    file = downloadPath + acc + "." + str(device) + ".csv"
                    tmpFile = file + ".bk"
                    utm_medium = ("Search" if device == 1 else "WapSearch")
                    urllib.urlretrieve(reportFilePath, tmpFile)
                    with open(file, "w") as output:
                        with codecs.open(tmpFile, "r", "gbk") as fileHandler:
                            index = 0
                            for line in fileHandler.readlines():
                                index += 1
                                if index == 1: # 剔除第一行
                                    continue
                                    
                                columns = line.encode("utf-8").strip("\r\n").split("\t")
                                newColumns = []
                                arr = [0, 2, 3, 4, 5, 6, 8, 9, 12, 10, 11, 14, 13, 15] # 目标表样对应数组下标
                                for index in arr:
                                    newColumns.append(columns[index])
                                newColumns[12] = newColumns[12].replace("%", "") # 点击率, '*%' -> '*'
                                newColumns[13] = newColumns[13].replace("-", "") # 排名, '-' -> ''
                                newColumns.append("Baidu")
                                newColumns.append(utm_medium)
                                newColumns.append(keyword.getCity(newColumns[3])) # 城市
                                output.write("\t".join(newColumns) + "\n")

            return 0
        except Exception, b:
            print "==> baidu error response %s" % response
            tb.print_exc()
            return 1

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "参数错误, Example: python baidu.py yyyy-mm-dd [downloadPath]"
        sys.exit(1)

    batchDate = sys.argv[1]
    downloadPath = os.path.split(os.path.realpath(__file__))[0] + "/download/baidu"
    if len(sys.argv) > 2:
        downloadPath = sys.argv[2]
    status = baidu().execute(batchDate, downloadPath)
    print "最终状态:%s" % status
    sys.exit(status)

