#coding=utf-8
from ApiSDKJsonClient import *


class sms_service_LiveReportService(ApiSDKJsonClient):

    def __init__(self):
        ApiSDKJsonClient.__init__(self, 'sms', 'service', 'LiveReportService')

    def getAccountLiveData(self, getAccountLiveDataRequest=None):
        return self.execute('getAccountLiveData', getAccountLiveDataRequest)

    def getKeywordLiveData(self, getKeywordLiveDataRequest=None):
        return self.execute('getKeywordLiveData', getKeywordLiveDataRequest)



