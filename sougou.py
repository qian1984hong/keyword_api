#!/usr/bin/python
# -*- coding: UTF-8 -*-

import traceback as tb
import sys
import os
import re
import time
import ConfigParser
import codecs
import keyword_script as keyword


''' sougou 模块 '''
class sougou():
    
    def __init__(self):
        print "===>启动 sougou模块"
        cf = ConfigParser.ConfigParser()
        cf.read("cfg.properties")
        sougouCfg = {}
        # key:value,key:value
        for data in cf.get("sougou-api", "utm_medium").strip().split(","):
            (key, value) = data.split(":")
            sougouCfg[key] = value
        self.sougouCfg = sougouCfg


    # request func(batchDate:yyyy-mm-dd)
    def execute(self, batchDate, downloadPath):
        try:
            if downloadPath[-1:] != "/":
                downloadPath += "/"
            response = None
            print "===>执行 sougou.execute(), batchDate %s" % batchDate
            scriptPath = os.path.split(os.path.realpath(__file__))[0]
            # 删除文件
            strCmd = "rm -f %s*.csv*" % downloadPath
            print strCmd
            os.system(strCmd)
            # jar包
            jarFile = scriptPath + "/sdk/sougou/sougou.jar"
            if not os.path.exists(jarFile):
                raise RuntimeError("文件不存在:%s" % jarFile)
            strCmd = "java -jar %s %s %s" %(jarFile, batchDate, downloadPath)
            print strCmd
            result = os.system(strCmd)
            if result != 0:
                raise RuntimeError("执行命令失败:" + strCmd)
            for acc in self.sougouCfg.keys():
                sourceFile = downloadPath + acc + ".csv.bk"
                print "===>处理文件:" + sourceFile
                utm_medium = self.sougouCfg[acc]
                utm_source = "Sogou"
                targetFile = sourceFile[:-3]
                with open(targetFile, "w") as output:
                    with codecs.open(sourceFile, "r", "gbk") as fileHandler:
                        index = 0
                        for line in fileHandler.readlines():
                            index += 1
                            if index in [1, 2]: # 第1,2行剔除
                                continue
                            # 处理"--"(空标识)
                            columns = line.replace("--", "").encode("utf-8").strip("\n").split(",")
                            if columns[11] == "": # 剔除"点击数"=""记录
                                continue
                            # cost:消耗, impression:展示数, click:点击数, cpc:点击均价, ctr:点击率, position:关键词排名
                            (cost, cpc, click, impression, ctr, position) = tuple(columns[9:16])
                            columns[9] = cost
                            columns[10] = impression
                            columns[11] = click
                            columns[12] = cpc
                            columns[13] = ctr.replace("%", "")
                            columns[14] = position
                            columns.pop(0) # 剔除"编号"
                            columns.append(utm_source) # 来源
                            columns.append(utm_medium) # 媒介
                            columns.append(keyword.getCity(columns[3])) # 城市
                            output.write("\t".join(columns) + "\n")
                            
            print "===>执行结果[Succ]"
            return 0
        except Exception, e:
            print e
            tb.print_exc()
            print "===>执行异常[Fail]"
            print "===>response is %s" % response
            return 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "参数错误, Example: python sougou.py yyyy-mm-dd [downloadPath]"
        sys.exit(1)

    batchDate = sys.argv[1]
    downloadPath = os.path.split(os.path.realpath(__file__))[0] + "/download/sougou"
    if len(sys.argv) > 2:
        downloadPath = sys.argv[2]
    status = sougou().execute(batchDate, downloadPath)
    print "最终状态:%s" % status
    sys.exit(status)
    
