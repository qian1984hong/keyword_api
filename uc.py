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
import keyword_script as keyword


''' uc类 '''
class uc():
    
    def __init__(self):
        print "===>启动 uc模块"
        cf = ConfigParser.ConfigParser()
        cf.read("cfg.properties")
        self.serverUrl = cf.get("uc-api", "serverUrl")
        self.username = unicode(cf.get("uc-api", "username"),"utf-8")
        self.password = cf.get("uc-api", "password")
        self.token = cf.get("uc-api", "token")


    #  request func(batchDate:yyyy-mm-dd)
    def execute(self, batchDate, downloadPath):
        print "===>执行 uc.execute(), batchDate %s" % batchDate
        if downloadPath[-1:] != "/":
            downloadPath += "/"
        strCmd = "rm -f %s*.csv*" % downloadPath
        print strCmd
        os.system(strCmd)
        response = None
        headers = {'content-type':'application/json;charset=utf-8'}
        try:
            header = AuthHeader(self.username, self.password, self.token, None)
            body = {"performanceData":["impression","click","cost","ctr","cpc"],"startDate":"%s" %batchDate,"endDate":"%s" %batchDate,"levelOfDetails":11,"unitOfTime":5,"reportType":14,"statRange":2}
            jsonEnv = JsonEnvelop(header,body)
            jsonStr=json.dumps(jsonEnv, default=convert_to_builtin_type, skipkeys=True)
            print "===>getReport() jsonStr is " + jsonStr
            # /api/report/getReport 创建报表任务
            url = self.serverUrl + "/api/report/getReport"
            response = requests.post(url, data=jsonStr, headers=headers).json() #{u'body': {u'status': u'CREATED', u'progress': 0.0, u'success': False, u'taskId': 1152921504759920086L}, u'header': {u'status': 0, u'leftQuota': 499977, u'quota': 500000, u'desc': u'success'}}
            taskId = response["body"]["taskId"] # 任务号
            
            # /api/task/getTaskState 获取报表状态
            body = {"taskId":taskId} 
            jsonEnv = JsonEnvelop(header,body)
            jsonStr=json.dumps(jsonEnv, default=convert_to_builtin_type, skipkeys=True)
            print "===>getTaskState() jsonStr is " + jsonStr
            url = self.serverUrl + "/api/task/getTaskState"
            response = requests.post(url, data=jsonStr, headers=headers).json()
            while response["body"]["status"] not in ["FINISHED", "FAILED"]:
                time.sleep(3)
                print "睡眠3秒...."
                response = requests.post(url, data=jsonStr, headers=headers).json()
            fileId = response["body"]["fileId"]
            
            # /api/file/download 下载报表
            body = {"fileId":fileId}
            jsonEnv = JsonEnvelop(header,body)
            jsonStr=json.dumps(jsonEnv, default=convert_to_builtin_type, skipkeys=True)
            print "===>download() jsonStr is " + jsonStr
            url = self.serverUrl + "/api/file/download"
            response = requests.post(url, data=jsonStr, headers=headers)
            source = downloadPath + "uc.csv.bk" 
            target = source[:-3]
            with open(source, "w") as o:
                o.write(response.text.encode("utf-8"))
            # 加载source文件, 做ETL清理
            with open(target, "w") as output:
                with open(source, "r") as fileHandler:
                    index = 0
                    for line in fileHandler.readlines():
                        index += 1
                        if index == 1: # 第1行剔除
                            continue
                        columns = line.strip("\r\n").split(",") # wins 换行符
                        # cost:消耗, impression:展示数, click:点击数, cpc:点击均价, ctr:点击率, position:关键词排名
                        (impression, click, cost, ctr, cpc) = tuple(columns[9:14])
                        columns[9] = cost
                        columns[10] = impression
                        columns[11] = click
                        columns[12] = cpc
                        columns[13] = ctr.replace("%", "")
                        columns.pop(1) # 剔除"账户ID"
                        columns.append("") # 关键字排名
                        columns.append("UC")
                        columns.append("WapSearch")
                        columns.append(keyword.getCity(columns[3])) # 城市                     
                        output.write("\t".join(columns) + "\n")

            print "===>执行结果[Succ]"
            return 0
        except Exception, e:
            tb.print_exc()
            print "===>执行异常[Fail]"
            print "===>response is %s" % response
            return 1

class JsonEnvelop():
    def __init__(self,aheader=None,abody=None): 
        self.header=aheader
        self.body=abody
    def setHeader(self,header):
        self.header=header
    def setBody(self,body):
        self.body=body

class AuthHeader():
    def __init__(self, username=None,password=None,token=None,target=None):
        self.username=username
        self.password=password
        self.token=token
        self.target=target
        

#转换函数
def convert_to_builtin_type(obj):
    # 把MyObj对象转换成dict类型的对象
    d = {}
    d.update(obj.__dict__)
    return d


if __name__ == "__main__":
    batchDate = sys.argv[1]
    downloadPath = os.path.split(os.path.realpath(__file__))[0] + "/download/uc"
    if len(sys.argv) > 2:
        downloadPath = sys.argv[2]
    status = uc().execute(batchDate, downloadPath)
    print "最终状态:%s" % status
    sys.exit(status)
    


