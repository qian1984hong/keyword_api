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
import requests
requests.packages.urllib3.disable_warnings()
import keyword_script as keyword


''' 奇虎360 模块 '''
class qihu360():

    # 字段列表
    colDictList = [("date","日期"),("type", "设备类型"), ("campaignId","推广计划ID"),("campaignName","推广计划"),
                    ("groupId","推广组ID"),("groupName","推广组"),("keywordId","关键词ID"),("keyword","关键词"),
                    ("views","展现数"),("clicks", "点击"),("totalCost","消费"),("avgPosition","关键词计算机排名"),
                    ("mavgPosition","关键词移动排名")]
    # 脚本所在目录
    scriptPath = os.path.split(os.path.realpath(__file__))[0]
    
    ''' 初始化 '''
    def __init__(self):
        print "===>启动 qihu360 模块"
        cf = ConfigParser.ConfigParser()
        cf.read("cfg.properties")
        # 用户名
        self.username = cf.get("qihu360-api", "username").strip()
        # 密码
        self.passwd = cf.get("qihu360-api", "passwd").strip()
        # apikey
        self.apiKey = cf.get("qihu360-api", "apiKey").strip()

        
    ''' 获取token '''
    def get_access_token(self):
        url = "https://api.e.360.cn/account/clientLogin"
        header = {
            "apiKey": self.apiKey,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        body = {
            "format": "json",
            "username": self.username,
            "passwd": self.passwd
        }
        rsp = requests.post(url, headers=header, data=body, verify=False)
        rsp_data = rsp.json()
        if "failures" in rsp_data:
            raise RuntimeError("获取access_token失败, 原因: " + rsp_data["failures"][0]["message"])
        else:
            print rsp_data["accessToken"]
            self.accessToken = rsp_data["accessToken"]

            
    ''' 主体逻辑 '''
    def execute(self, batchDate, downloadPath):
        try:
            if downloadPath[-1:] != "/":
                downloadPath += "/"
            self.get_access_token()
            strCmd = "rm -f %s*.csv*" % downloadPath
            print strCmd
            os.system(strCmd)
            header = {
                "apiKey": self.apiKey,
                "accessToken": self.accessToken,
                "Content-Type": "application/x-www-form-urlencoded"
            }
            body = {
                "format": "json",
                "startDate": batchDate,
                "endDate": batchDate,
                "level": "account",
                "type": "all"
            }
            sourceFile = downloadPath + self.username + ".csv.bk"
            targetFile = sourceFile[:-3]
            # 查询分页数
            url = "https://api.e.360.cn/dianjing/report/keywordCount"
            rsp = requests.post(url, headers=header, data=body, verify=False)
            rsp_data = rsp.json()
            if "failures" in rsp_data:
                print rsp_data
                raise RuntimeError("获取分页数失败, 原因: " + rsp_data["failures"][0]["message"])
            totalPage = rsp_data["totalPage"]
            # 获取数据
            url = "https://api.e.360.cn/dianjing/report/keyword"
            with open(sourceFile, "w+") as output:
                output.write("日期,设备类型,推广计划ID,推广计划名称,推广组ID,推广组名称,关键词ID,关键词,展现,点击,消费,关键词计算机排名,关键词移动排名\n")
                for page in range(1, totalPage+1):
                    body["page"] = page
                    rsp = requests.post(url, headers=header, data=body, verify=False)
                    rsp_data = rsp.json()
                    if "failures" in rsp_data:
                        raise RuntimeError("抽取数据失败, 原因: " + rsp_data["failures"][0]["message"])
                    for json in rsp_data["keywordList"]:
                        list = []
                        for colDict in self.colDictList:
                            key = colDict[0]
                            list.append(str(json[key]))
                        output.write(",".join(list) + "\n")
            print "生成文件:" + sourceFile
            # source -> target
            with open(targetFile, "w") as output:
                with open(sourceFile, "r") as fileHandler:
                # 原始表样: 日期,设备类型,推广计划ID,推广计划名称,推广组ID,推广组名称,关键词ID,关键词,展现,点击,消费,关键词计算机排名,关键词移动排名
                # 目标表样: 日期,账户,推广计划ID,推广计划,推广组ID,推广组,关键词id,关键词,消耗,展示数,点击数,点击均价,点击率,关键词排名,utm_source,utm_medium,city
                    index = 0
                    for line in fileHandler.readlines():
                        index += 1
                        if index == 1:
                            continue
                        columns = line.strip("\n").split(",")
                        list = [columns[0], self.username, columns[2], columns[3], columns[4], columns[5], columns[6], columns[7]]
                        # cost:消耗, impression:展示数, click:点击数, cpc:点击均价, ctr:点击率, position:关键词排名
                        cost,impression,click = float(columns[10]),int(columns[8]),int(columns[9])
                        if click == 0: # 剔除 "点击数" 为 0
                            continue
                        else:
                            ctr = format(float(click*100)/impression, '.2f')
                            cpc = format(cost/click, '.2f')
                            list.append(str(cost))
                            list.append(str(impression))
                            list.append(str(click))
                            list.append(cpc)
                            list.append(ctr)
                            list.append("")
                            list.append("360") # utm_source
                            if columns[1] == "computer":
                                list.append("Search")
                            elif columns[1] == "mobile":
                                list.append("WapSearch")
                            else:
                                list.append("")
                            list.append(keyword.getCity(columns[3]))
                            output.write("\t".join(list) + "\n")
            print "===>执行结果[Succ]"
            return 0
        except Exception, e:
            tb.print_exc()
            print "===>执行异常[Fail]"
            return 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "参数错误, Example: python qihu360.py yyyy-mm-dd [downloadPath]"
        sys.exit(1)

    batchDate = sys.argv[1]
    downloadPath = os.path.split(os.path.realpath(__file__))[0] + "/download/360"
    if len(sys.argv) > 2:
        downloadPath = sys.argv[2]
    status = qihu360().execute(batchDate, downloadPath)
    print "最终状态:%s" % status
    sys.exit(status)
    


