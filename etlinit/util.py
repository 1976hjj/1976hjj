# -*- coding: utf-8 -*-
from etltool import *
#import sys
#import re
#import os
#from pyspark.sql.types import StructType, StructField, StringType
os.getpid()
reload(sys)
sys.setdefaultencoding('utf8')
#import pyspark.sql.functions as f
#from pyspark.sql import dataframe
#import subprocess
#import logger
#import ConfigParser
cf = ConfigParser.ConfigParser()
try:
    cf.read('/home/sys_sqds/scripts/config.ini')
    parentPath = cf.get("hadoop", "parentPath")
    rawdatapath = cf.get("hadoop", "path")
    groupNum = cf.get("rule", "groupNum")
    measure = cf.get("rule", "measurefilecontain")
    tolerance = cf.get("rule", "tolerancefilecontain")
except Exception,e:
    logger.logger.error(e)
#from hive import *
datestr = time.strftime('%Y-%m-%d', time.localtime(time.time()))
spark = SparkSession.builder.master("local[8]").enableHiveSupport().appName("test").getOrCreate()


def getIdSupplierNameAndPyName():
    try:
        sdf = getConfigFromTable("rulevalid")
        sdf = sdf.select("id","mail_receiver","python_file_name")
        #python_file_name
        sdf = sdf.withColumn("supplier_name",f.split("python_file_name",",")[0])
        sdf = sdf.withColumn("python_name",f.split("python_file_name",",")[1])
        sdf = sdf.select("id","supplier_name","python_name")
        return sdf
    except Exception, e:
        logger.logger.error(e)

def getMeasuredataFileName():
    try:
        rawdatacmd = 'hdfs dfs -find {} -name *.csv'.format(rawdatapath)
        rawdatafiles = subprocess.check_output(rawdatacmd, shell=True).strip().split(',')
        rawdatafilesets = set(rawdatafiles)
        tmmeasuredata = cf.get("hadoop","tmmeasuredata")
        tdmeasuredim = cf.get("hadoop","tdmeasuredim")
        measurepath = parentPath + tmmeasuredata
        dimpath = parentPath + tdmeasuredim

        measuredatacmd = 'hdfs dfs -find {} -name *.csv'.format(measurepath)
        measurefiles = subprocess.check_output(measuredatacmd, shell=True).strip().split(',')
        measurefilesets = set(measurefiles)
        dimcmd = 'hdfs dfs -find {} -name *.csv'.format(dimpath)
        dimfiles = subprocess.check_output(dimcmd, shell=True).strip().split(',')
        dimfilesets = set(dimfiles)
        diffset = measurefilesets.union(dimfilesets).difference(rawdatafilesets)
        return diffset
    except Exception ,e:
        logger.logger.error(e)


#获取csv文件名通过日期
# def getCsvNameListByDate(id,dateStr,startdate):
#     try:
#         files = getMeasuredataFileName()
#         todayfiles = []
#         for path in files:
#             csvname = getlastslashstr(path,"/")
#             #校验是否csv
#             if validcsvname(csvname):
#                 #获取文件名，不包含csv
#                 filename = getfirstslashstr(csvname,".")
#                 dateinfile = getlastslashstr(filename,"-")
#                 if dateinfile == dateStr:
#                     todayfiles.append(csvname)
#             else:
#                 cpAndInsert(id,startdate,path,"文件类型错误")
#         return todayfiles
#     except Exception, e:
#         logger.logger.error(e)

def cpMeasureData(path):
    try:
        textname = getlastslashstr(path, "/")
        subprocess.call(["hadoop", "fs", "-cp", parentPath + "/rawdata/" + textname, parentPath + "/tm_measure_data/" + textname])
    except Exception,e:
        logger.logger.error(e)

def cpMeasureDim(path):
    try:
        textname = getlastslashstr(path, "/")
        subprocess.call(["hadoop", "fs", "-cp", parentPath + "/rawdata/" + textname, parentPath + "/td_measure_dim/" + textname])
    except Exception,e:
        logger.logger.error(e)

def getMeasuredataFileName():
    try:
        rawdatacmd = 'hadoop fs -find {} -name "*.csv"'.format(rawdatapath)
        rawdatafiles = subprocess.check_output(rawdatacmd, shell=True).strip().split('\n')
        rawdatafilesets = set(rawdatafiles)
        print rawdatafilesets
        tmmeasuredata = cf.get("hadoop","tmmeasuredata")
        tdmeasuredim = cf.get("hadoop","tdmeasuredim")
        measurepath = parentPath + tmmeasuredata
        dimpath = parentPath + tdmeasuredim

        measuredatacmd = 'hadoop fs -find {} -name "*.csv"'.format(measurepath)
        measurefiles = subprocess.check_output(measuredatacmd, shell=True).strip().split('\n')
        measurefilesets = set(measurefiles)
        print measurefilesets
        dimcmd = 'hadoop fs -find {} -name "*.csv"'.format(dimpath)
        dimfiles = subprocess.check_output(dimcmd, shell=True).strip().split('\n')
        dimfilesets = set(dimfiles)
        print dimfilesets
        diffset = rawdatafilesets.difference(measurefilesets.union(dimfilesets))
        return diffset
    except Exception,e:
        logger.logger.error("getMeasuredataFileName:")
        logger.logger.error(e)

#利用subprocess 执行shell脚本 复制 且 删除文件
def cpAndInsert(taskid,startdate,path,errorType):
    try:
        textname= getlastslashstr(path,"/")
        subprocess.call(["hadoop", "fs", "-cp", parentPath + "/rawdata/" + textname, parentPath + "/error/" + textname])
        subprocess.call(["hadoop", "fs", "-rm", "-f",parentPath + "/rawdata/" + textname])
        resultjson = """{'hasSuccess':false,'errorType':'%s','path':'%s'}"""%(errorType,path)
        insertIntoResult(taskid,startdate,resultjson)
    except Exception, e:
        logger.logger.error(e)

#保存到结果表中
def saveResultFromDF(df):
    result = ""
    id = ""
    try:
        # 判断是否df
        if isinstance(df, dataframe):
            if len(df.head(1)) > 0:
                ids = [row['id'] for row in df.select('id').collect()]
                configList = getConfigByIds(ids)
                for row in df.collect():
                    id = row['id']
                    if judgeColumnExist("hasSuccess",df):
                        # 成功
                        if df["hasSuccess"] == True:
                            result = """{'hasSuccess':%s,'id':'%s','update_time':'%s','supplier_name':'%s',
                            'errorType':'%s'}"""%(row['hasSuccess'],id,row['update_time'],row["supplier_name"])
                        else:
                            mail_receiver_collect = configList.where(configList['id'] == row['id']).select('mail_receiver').collect()
                            check_desc_collect = configList.where(configList['id'] == row['id']).select('check_desc').collect()
                            mail_receiver = [row['mail_receiver'] for row in mail_receiver_collect]
                            check_desc = [row['check_desc'] for row in check_desc_collect]
                            result = """{'hasSuccess':%s,'id':'%s','update_time':'%s','supplier_name':'%s'
                            ,'errorDesc':'%s','mail_receiver':'%s','errorResults':'%s'
                            }"""%(row['hasSuccess'],id,row['update_time'],row["supplier_name"],
                                 check_desc[0],mail_receiver[0],row['errorResults'])
                    else:
                        #结果里没有是否成功这个字段
                        result = """{'hasSuccess':false,'id':'%s','update_time':'%s','supplier_name':'%s'
                            ,'errorDesc':'%s'
                            }"""%(id,row['update_time'],row["supplier_name"],'hasSuccess has no value')
                    insertIntoResult(row['id'],datestr,result)
    except Exception,e:
        logger.logger.error(e)

def judgeColumnExist(rowName,df):
    if rowName in df.columns:
        return True
    else:
        return False


def insertIntoResult(taskid,startdate,resultjson):
    insertIntoResultTable(taskid, startdate, resultjson, "1", "error")

#将分隔符替换为空
def replacestrslash( text, slash):
    if(slash in text):
        return text.replace(slash, "")
    else:
        return text

#如果有斜杠/，分割获取最后一个斜杠/后的字符串
def getlastslashstr( text, slash):
    if(slash in text):
        text = text.split(slash)
        text = text[text.__len__() - 1]
        return text
    else:
        return text

# 如果有斜杠/，分割获取最后一个斜杠/后的字符串
def getfirstslashstr( text, slash):
    if (slash in text):
        text = text.split(slash)
        text = text[0]
        return text
    else:
        return text


#校验是否为csv
def validcsvname(csvname):
    try:
        if(csvname.split(".",1)[1] != "csv"):
            return False
        else:
            return True
    except Exception, e:
        logger.logger.error(e)


#用-分割
def splitcsvname(csvname,slash):
    try:
        csvname = getlastslashstr(csvname, "/")
        if(slash in csvname):
            return csvname.split(slash)
        else:
            return csvname
    except Exception, e:
        logger.logger.error(e)


#校验供应商名称和第一列是否一致，比较时都改为大写
def validsuppliername(path,suppliername,supplierNameRow):
    try:
        list = supplierNameRow.distinct().collect()
        if(list.__len__() == 1):
            if(list[0][0].upper() == suppliername.upper()):
                return True
            else:
                return False
        else:
            return False
    except Exception, e:
        logger.logger.error(e)


#时间字符串转换为时间戳
def timestrtotimestamp(timestr):
    timestr = re.sub("\D","",timestr)
    try:
        timearray = time.strptime(timestr, "%Y%m%d")
        return int(time.mktime(timearray))
    except Exception ,e:
        logger.logger.error(e)
    else:
        logger.logger.error("unknown error")



#校验文件名中时间和文件内update_time是否一样
def validupdatetime(path, updatetimestr, row):
    list = row.distinct().collect()
    if (list.__len__() == 1):
        if timestrtotimestamp(updatetimestr) == timestrtotimestamp(list[0][0]):
            return True
        else:
            return False
    else:
        return False

#校验包含measure的csv中是否数据是一组5个
def validmeasurecsvgroupnum(path,row):
    list = row.groupBy(["supplier_name","Part_name","part_number","key_feature","sample_batch"]).count().collect()
    for x in list:
        if x['count'] != groupNum:
            break
            return False
        else:
            continue
    return True

# 用于标记正确信息
def getSuccess(id, supplier_name, update_time):
    try:
        # 没有任何错误数据,要展示的结果
        success_schema = StructType([StructField("hasSuccess", StringType()),
                                 StructField("id", StringType()),
                                 StructField("supplier_name", StringType()),
                                 StructField("update_time", StringType())])
        rdd = spark.sparkContext.parallelize([('true', id, supplier_name, update_time)])
        success_df = spark.createDataFrame(rdd, success_schema)
        return success_df
    except BaseException, e:
        logger.logger.error(e)


# 结合id，supplier_name,mail_receiver转成一个dataframe
def getWrong_one(id, supplier_name,  update_time,json_str):
    try:
        # 有错误数据，要展示的结果
        wrong_schema = StructType([StructField("hasSuccess", StringType()),
                                    StructField("errorType", StringType()),
                                    StructField("id", StringType()),
                                    StructField("supplier_name", StringType()),
                                    StructField("update_time", StringType()),
                                    StructField("mail_receiver", StringType()),
                                    StructField("errorResults", StringType())])
        # print json_str
        rdd = spark.sparkContext.parallelize(
            [('false', 'rulevalid', id, supplier_name, update_time,json_str)])
        wrong_df = spark.createDataFrame(rdd, wrong_schema)
        return wrong_df
    except BaseException, e:
        logger.logger.error(e)

# 判断是否跑成功，获取结果dataframe转成的json，判断长度是否为0，0则表明没有错误数据。不为0，表明有错误数据.
def judgeSuccess(id, supplier_name, update_time,data_df):
    try:
        json_str = data_df.toJSON().collect()
        if len(json_str) == 0:
            return getSuccess(id, supplier_name, update_time)
        else:
            return getWrong_one(id, supplier_name, update_time, json_str)
    except BaseException, e:
        logger.logger.error(e)


# 用于获取配置文件的参数，因为传过来的是一个dataframe，需要解析
def getPara(x):
    id=x[0]
    supplier=x[1]
    python_name=x[2]
    file_name_use=sys.argv[0]
    if python_name == file_name_use:
        return [id, supplier]

