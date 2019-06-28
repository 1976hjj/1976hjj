# -*-coding:utf-8 -*-


from bigdatatool import *

#文件名上传时间需要与文件中update_time一致，时间为实际上传时间
#每组3，5个样本
#文件内容格式

class FileCheck:

    sc = sc  #类变量spark上下文环境
    FilePath = ''

    #构造函数
    def __init__(self):
        self.__CheckResult = {}  #检查结果



    def printsc(self):
        print(self.sc)
       # print(self.FilePath)



    #某元素与大数据文件某值(通过spark_sql查到)保持一致
    # test_case
    # sqds_check=FileCheck()
    # re=re=sqds_check.match_csvdata(['1','2','3','4'],'/user/hive/warehouse_ext/DL/sqds/rawdata/tm_measure_data/sds-constantspeeddriveshaft-measuredate-20190522.csv','select distinct sample_numble from csvdf')
    #print(re)
    #sc.stop()
    def match_csvdata(self,content_li,filepath,spark_sql,encoding='GBK',header='true'):
        try:
          matcheddata = []
          content_li = list(content_li)
          if   stringtool.getlastslashstr(filepath,'.')=='csv':
               csvdf=sc.read.csv(filepath,encoding=encoding,header=header)
               csvdf.registerTempTable('csvdf')
               matcheddata_df=sc.sql(spark_sql)
               #matcheddata_df.collect()
               for i in matcheddata_df.collect():
                   matcheddata.append(i[0])
               difference_re1=set(matcheddata).difference(set(content_li))
               difference_re2=set(content_li).difference(set(matcheddata))
          else:
               logging.fatal('FileCheck not support except csv')
               return False
        except (Exception,BaseException) as e:
               logging.fatal(e)
        if  (('difference_re1' in dir())
                and ('difference_re1' in dir())
                and (len(difference_re1)==0)
                and (len(difference_re2)==0)):
               return True
        else:
               return False

sqds_check=FileCheck()
sqds_check.printsc()
