#!/usr/bin/python
# -*- coding: UTF-8 -*-

import traceback as tb
import time
import sys
import os
import re
import codecs
import ConfigParser
''' import uc '''
from uc import *
''' import sougou '''
from sougou import sougou
''' import baidu '''
from baidu import baidu
''' import toutiao '''
from toutiao import *
''' import 360 '''
from qihu360 import *
''' imprt mysq util '''
sys.path.append('/pub/etlProject/scripts/pyUtil')
import mysqlUtil
import hiveUtil
from hdfsUtil import MyWebHDFS



##--------------------------------------------------------------------
##程序名称: keyword.py
##执行环境: Linux
##程序描述: 搜索引擎.关键字.抽数脚本
##传入参数: 批次[batchTime]
##命令举例: python keyword.py 20190701
##产生实体: ./download/
##----------------------------------------------------------------------

# 脚本所在目录
scriptPath = os.path.split(os.path.realpath(__file__))[0]
# 下载主目录
downloadRootPath = scriptPath + "/download/"

# 城市列表
cityList = ["上海","北京","深圳","南京","常州","无锡","武汉","成都","大连","太原","重庆","昆明","广州","贵阳","哈尔滨","杭州","合肥",
            "东莞","佛山","福州","呼和浩特","湖州","惠州","吉林","济南","嘉兴","金华","南昌","南宁","南通","宁波","青岛","泉州","三亚",
            "厦门","绍兴","沈阳","石家庄","苏州","台湾","台州","泰州","唐山","天津","潍坊","温州","芜湖","西安","湘潭","襄阳","新加坡",
            "徐州","烟台","营口","张家港","长春","长沙","镇江","郑州","淄博","乌鲁木齐","银川"]


# 根据${line}, 获取${cityList}中对应的城市
def getCity(line):
    for city in cityList:
        if line.find(city) >= 0:
            return city
    return None

''' 抽取逻辑 '''
def extract_file(batchDate):

    ''' sougou 模块 '''
    downloadPath = downloadRootPath + "sougou"
    status = sougou().execute(batchDate, downloadPath)
    if status:
        raise RuntimeError("===> sougou 模块执行异常")
        
    ''' uc 模块 '''
    downloadPath = downloadRootPath + "uc"
    status = uc().execute(batchDate, downloadPath)
    if status:
        raise RuntimeError("===> uc 模块执行异常")

    ''' baidu 模块 '''
    downloadPath = downloadRootPath + "baidu"
    status = baidu().execute(batchDate, downloadPath)
    if status:
            raise RuntimeError("===> baidu 模块执行异常")
        
    ''' toutiao 模块 '''
    downloadPath = downloadRootPath + "toutiao"
    status = toutiao().execute(batchDate, downloadPath)
    if status:
        raise RuntimeError("===> toutiao 模块执行异常")
        
    ''' 360 模块 '''
    downloadPath = downloadRootPath + "360"
    status = qihu360().execute(batchDate, downloadPath)
    if status:
        raise RuntimeError("===> 360 模块执行异常")


''' 表:pub_traffic_utm_rebate, 获取返点数据 '''
def get_rebate_dict(batchDate):
    cf = ConfigParser.ConfigParser()
    cf.read("cfg.properties")
    dbAddress = cf.get("common-api", "db_address").strip()
    dbUser = cf.get("common-api", "db_user").strip()
    dbPassword = cf.get("common-api", "db_password").strip()
    dbSid = cf.get("common-api", "db_sid").strip()
    dbPort = cf.get("common-api", "db_port").strip()
    (retCode, retObject) = mysqlUtil.createDBConn(dbAddress, dbUser, dbPassword, dbSid, dbPort)
    if retCode != 0:
        raise RuntimeError("func get_rebate_dict(): " + retObject)

    dbConn = retObject # 数据库连接对象
    strSql = "SELECT CONCAT(utm_source, '-', utm_medium) AS utm_key, rebate FROM pub_traffic_utm_rebate t WHERE starttime <='%s' AND endtime >= '%s'" %(batchDate, batchDate)
    print strSql
    (retCode, retObject) = mysqlUtil.queryDB(dbConn, strSql)
    if retCode != 0:
        raise RuntimeError("func get_rebate_dict(): " + retObject)
    utmDict = {} # utm_key:rebate
    for data in retObject:
        utmDict[data[0]] = float(data[1])
        
    dbConn.close()
    return utmDict

    
''' 合并文件, 处理"返点逻辑" '''
def merge_file(batchDate):
    sourceFile = downloadRootPath + batchDate + ".csv.bk"
    strCmd = 'cat `find %s*/ -type f -name "*.csv"` > %s' %(downloadRootPath, sourceFile)
    print "合并文件: " + strCmd
    os.system(strCmd)
    targetFile = sourceFile[:-3]
    utmDict = get_rebate_dict(batchDate)
    with open(targetFile, "w") as output:
        with open(sourceFile, "r") as fileHandler:
            for line in fileHandler.readlines():
                columns = line.strip("\n").split("\t")
                # 花费,来源-媒介
                cost = float(columns[8])
                utmKey = columns[14] + "-" + columns[15]
                utmValue = 0
                if utmKey in utmDict:
                    utmValue = utmDict[utmKey]
                cost2 = format(cost/(1 + utmValue), '.2f')
                output.write(line[:-1] + "\t" + cost2 + "\t" + str(utmValue) + "\n")


# 检查分区是否存在(分区名规则:partitionKey=partitionValue)
def checkHivePartitions(hiveConn, table, partitionKey, partitionValue):
    partitionStr = "%s=%s" %(partitionKey, partitionValue)
    strSql = "show partitions " + table
    (retCode, retObject) = hiveUtil.queryDB(hiveConn, strSql)
    if retCode != 0:
        raise RuntimeError("查询分区错误:" + retObject)
    isExists = False
    for line in retObject:
        if line[0].find(partitionStr) != -1:
            isExists = True
            break
    if isExists == False:
        strSql = "alter table " + table + " add partition (" + partitionStr + ")"
        print strSql
        (retCode, retObject) = hiveUtil.executeDB(hiveConn, strSql)
        if retCode != 0:
            hiveUtil.closeConn(hiveConn)
            raise RuntimeError("创建分区失败:" + retObject)
        else:
            print "创建分区:" + partitionStr
    else:
        hiveUtil.closeConn(hiveConn)
        print "分区已经存在, 跳过操作"

        
''' 检查hive分区, 上传hdfs文件 '''
def checkPartitionAndUpload(batchDate):
    cf = ConfigParser.ConfigParser()
    cf.read("cfg.properties")
    # hive 检查分区
    hiveHost = cf.get("common-api", "hive_host").strip()
    hiveUser = cf.get("common-api", "hive_user").strip()
    hivePassword = cf.get("common-api", "hive_password").strip()
    hiveSchema = cf.get("common-api", "hive_schema").strip()
    hivePort = cf.get("common-api", "hive_port").strip()
    (retCode, retObject) = hiveUtil.createDBConn(hiveHost, hiveUser, hivePassword, hiveSchema, hivePort)
    if retCode != 0:
        raise RuntimeError("创建hive连接失败")
    hiveConn = retObject
    partitionKey = "batch_month"
    partitionValue = batchDate[:4] + batchDate[5:7]
    table = "odl_search_engine_keyword_detail"
    checkHivePartitions(hiveConn, table, partitionKey, partitionValue)
    # hdfs 上传文件
    hadooptAciveNodePath = cf.get("common-api", "hadoop_active_node").strip()
    strCmd = "tail -1 " + hadooptAciveNodePath
    activeNode = os.popen(strCmd).read().strip()
    print "activeNode is " + activeNode
    hadoopPort = cf.get("common-api", "hadoop_port").strip()
    hadoopUsername = cf.get("common-api", "hadoop_username").strip()
    source = downloadRootPath + "/" + batchDate + ".csv"
    target = "/user/hive/warehouse2/" + table + "/" + partitionKey + "=" + partitionValue + "/" + batchDate + ".csv"
    print "hdfs 上传文件至:" + target
    webhdfs = MyWebHDFS(activeNode, hadoopPort, hadoopUsername)
    webhdfs.copyFromLocal(source, target)
    
   
''' 压缩打包文件 '''   
def zip_clean(batchDate):
    strCmd = '''
                cd %s
                tar czvf %s/history/%s.tar.gz . >/dev/null 2>&1
            '''  %(downloadRootPath, scriptPath, batchDate)
    print "压缩文件: " + strCmd
    os.system(strCmd)
    strCmd = 'rm `find %s -type f `' % downloadRootPath
    print "删除原文件: " + strCmd
    os.system(strCmd)
    

''' 入口函数 '''
if __name__ == "__main__":
    try:
        if len(sys.argv) != 2 or not re.match('^\d{8}$', sys.argv[1].strip()):
            script = os.path.basename(sys.argv[0]).split(".")[0]
            print "参数无效或错误, Example: python %s.py yyyymmdd" % script
            sys.exit(1)
            
        batchDate = sys.argv[1][:4] + "-" + sys.argv[1][4:6] + "-" + sys.argv[1][-2:]
        print "启动[搜索引擎-关键字提取]脚本, batchDate=%s" % batchDate
        strCmd = "rm -f %s*.csv*" % downloadRootPath
        print strCmd
        os.system(strCmd)
        # 执行抽取程序
        extract_file(batchDate)
        # 合并,处理"返点"
        merge_file(batchDate)
        # 检查分区, 上传文件
        checkPartitionAndUpload(batchDate)
        # 压缩文件
        zip_clean(batchDate)
        print "执行成功[Succ]"
        sys.exit(0)
        
    except Exception, e:
        print e
        print "执行失败[Fail]"
        tb.print_exc()
        sys.exit(1)

