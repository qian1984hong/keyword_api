keyword-api 搜索引擎api-关键字-提取程序
====

* 本文目的:
	* 本文对该项目功能,脚本作用做相关介绍, 方面后期维护及升级
* 功能说明:
	* 根据指定日期(T-1不支持跨天抽取),抽取增量数据"Baidu,UC,360,今日头条,Sougou)
	* ETL清洗
	* 合并文件(合并后的文件格式:YYYY-MM-DD.csv), 并上传到hive指定分区表指定分区下
* 其他说明:
	* 由于今日头条的token有效期为30天,因此每**30天**需要手工点击[回调URL](https://ad.oceanengine.com/openapi/audit/oauth.html?app_id=1638129195871244&state=your_custom_params&scope=%5B4%5D&redirect_uri=http%3A%2F%2Fdata.daoxila.com%2Fapi%2Fauth%2Ftoutiao%2Fcallback "https://ad.oceanengine.com/openapi/audit/oauth.html?app_id=1638129195871244&state=your_custom_params&scope=%5B4%5D&redirect_uri=http%3A%2F%2Fdata.daoxila.com%2Fapi%2Fauth%2Ftoutiao%2Fcallback")
	* 工程目录:/pub/etlProject/scripts/adhoc/keyword_api

## 目录结构说明

### 渠道api接口
* baidu.py: 百度
* qihu360.py: 奇虎360
* sougou.py: 搜狗
* toutiao.py: 今日头条
* uc.py: UC游览器
* 备注:
	* 上诉接口除了 toutiao.py外, 其余个接口独立运行样式: python xxx.py YYYY-MM-DD downloadPath
	* toutiao.py 提供两个功能:
		* 刷新token(此功能通常供回调url调用): python toutiao.py refresh_token authCode
		* 抽取数据: python toutiao.py execute YYYY-MM-DD [downloadPath]
	* 参数:downloadPath 可选, 如不指定, 默认目录: 工程主目录/download
    
### 脚本
* keyword_script.py: **主程序**
* toutiao_refresh_token.sh: 今日头条回调url脚本(springboot会接受请求,并予以调用[http://192.168.24.60:8080/api/auth/toutiao/callback])

### 配置文件
* cfg.properties:(相关搜索引擎配置信息都在此文件集中配置)

### 目录说明
* download: 文件下载目录
	* baidu: 百度文件目录
	* 360: 奇虎360目录
	* sougou: 搜狗目录
	* toutiao: 今日头条目录
	* uc: UC游览器目录
* history: download压缩归档目录(形式:YYYY-MM-DD.tar.gz)
* sdk: 源码包目录
	* baidu: 百度源码示例(Python)
	* sougou: 搜狗源码示例(Java)

### 相关搜索引擎API帮助文档
* 百度:[http://dev2.baidu.com/newdev2/dist/index.html#/content/?pageType=3&productlineId=3](http://dev2.baidu.com/newdev2/dist/index.html#/content/?pageType=3&productlineId=3)
* 奇虎360:[http://open.e.360.cn/api/report_accountdaily.html](http://open.e.360.cn/api/report_accountdaily.html)
* 搜狗:[http://apihome.sogou.com/document/ss/doc1-1.jsp](http://apihome.sogou.com/document/ss/doc1-1.jsp)
* 今日头条:[https://ad.toutiao.com/openapi/doc/index.html?id=187 ](https://ad.toutiao.com/openapi/doc/index.html?id=187 )
* UC游览器:[https://open2.sm.cn/static/openweb/#/techdoc/1](https://open2.sm.cn/static/openweb/#/techdoc/1)