# -*-coding:utf-8 -*-

from han_test import *
from han_test import sc
from han_test import stringtool

#文件名上传时间需要与文件中update_time一致，时间为实际上传时间
#每组3，5个样本
#文件内容格式

class FileCheck:

    sc = sc  #类变量spark上下文环境
    FilePath = ''
    def __init__(self):
        self.__CheckResult = {}  #检查结果

    def printsc(self):
        print(self.sc)
        print(self.FilePath)

    #某元素与大数据文件某值(通过spark_sql查到)保持一致
    def match_csvdata(self,content_li,filepath,spark_sql,encodng='GBK',header='true'):
        matcheddata=[]
        content_li=list(content_li)
        try:
          if   stringtool.getlastslashstr(filepath,'.')=='csv':
               csvdf=sc.read.csv(filepath,encoding=encodng,header=header)
               csvdf.registerTempTable('csvdf')
               matcheddata_df=sc.sql(spark_sql)
               for i in matcheddata_df.collect():
                   matcheddata.append(i[0])
               difference_re1=set(matcheddata).difference(set(content_li))
               difference_re2=set(content_li).difference(set(matcheddata))
               print(difference_re1)
               print(difference_re2)
          else:
               logging.fatal('FileCheck not support except csv')
               return False
        except (Exception,BaseException) as e:
               logging.FATAL(e)
        if  (('difference_re1' in dir())
                and ('difference_re1' in dir())
                and (len(difference_re1)==0)
                and (len(difference_re2)==0)):
               return True
        else:
               return False

sqds_check=FileCheck()
re=sqds_check.match_csvdata(['89','87','90','82','86','81','84','88','83','80','99'],'/user/hive/warehouse_ext/DL/sqds/rawdata/tm_measure_data/smts-constantspeeddriveshaft-measureddate-20190524.csv','select distinct measure_data from csvdf')
print(re)

