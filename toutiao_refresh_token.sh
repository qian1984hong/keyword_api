#!/bin/bash

#功能: 今日头条, 刷新token

source /home/hadoop/.bash_profile
cd /pub/etlProject/scripts/adhoc/keyword_api
python toutiao.py refresh_token $1
exit $?
