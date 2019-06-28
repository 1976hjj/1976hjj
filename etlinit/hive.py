# -*- coding: utf-8 -*-
#from pyspark.sql import SparkSession
#import time
#import datetime
from han_test import *
#import sys
reload(sys)
sys.setdefaultencoding('utf8')
datestr = time.strftime('%Y%m%d', time.localtime(time.time()))
dateslashstr = time.strftime('%Y-%m-%d', time.localtime(time.time()))
spark = SparkSession.builder.master("local").enableHiveSupport().appName("test").getOrCreate()
#import logger

#通过校验类型从表里读取配置，每个取创建时间最新的数据
def getConfigFromTable(validtype):
    try:
        now = datetime.datetime.now()
        day = now.weekday()
        spark.sql("use dl_sqds")
        sql="""
            select cfg.id,cfg.check_name,cfg.check_desc,cfg.check_frequency,
            cfg.mail_receiver,cfg.output_ok_result,
            cfg.python_file_name,cfg.is_effective, cfg.src_create_date, 
            '%s' as update_time
            from (
            select rn.check_name,rn.src_create_date from (
            select f.check_name,f.src_create_date,row_number() over (partition by f.check_name order by f.src_create_date desc) as rownum 
            from dl_sqds.tc_sqds_notice_cfg f 
            where valid_type='%s'
            and locate('%s',f.check_frequency) > 0
            ) rn where rn.rownum = 1 ) frn left join dl_sqds.tc_sqds_notice_cfg cfg 
            on frn.check_name=cfg.check_name and frn.src_create_date=cfg.src_create_date 
        """%(dateslashstr,validtype,day)
        return spark.sql(sql)
    except Exception,e:
        logger.logger.error(e)

#根据id获取配置信息
def getConfigById(id):
    try:
        spark.sql("use dl_sqds")
        sql = """
            select
            f.*
            from dl_sqds.tc_sqds_notice_cfg f 
            where id='%s'
        """%(id)
        return spark.sql(sql)
    except Exception,e:
        logger.logger.error(e)

#根据id获取配置信息
def getConfigByIds(ids):
    ids = ','.join(ids)
    try:
        spark.sql("use dl_sqds")
        sql = """
            select
            f.*
            from dl_sqds.tc_sqds_notice_cfg f 
            where id in (%s)
        """%(ids)
        return spark.sql(sql)
    except Exception,e:
        logger.logger.error(e)

#往调度记录表中插入数据
def insertIntoResultTable(taskId,startdate,resultJson,isEffective,loadFrom):
    try:
        spark.sql("use dl_sqds")
        sql = """
            insert into dl_sqds.ta_sqds_notice_result
            select 
                '%s' as ID  , 
                '%s' as CHECK_DATE,
                '%s' as CHECK_RESULT,
                '%s' as IS_EFFECTIVE,
                unix_timestamp(from_unixtime(unix_timestamp(current_timestamp()),"yyyy-MM-dd HH:mm:ss"))  as src_create_date,
                unix_timestamp(from_unixtime(unix_timestamp(current_timestamp()),"yyyy-MM-dd HH:mm:ss")) as SRC_UPDATE_DATE,
                unix_timestamp(from_unixtime(unix_timestamp(current_timestamp()),"yyyy-MM-dd HH:mm:ss")) as FIRST_LOAD_DATE,
                unix_timestamp(from_unixtime(unix_timestamp(current_timestamp()),"yyyy-MM-dd HH:mm:ss")) as LAST_LOAD_DATE,
                '%s' as LOAD_FROM
            """%(taskId,startdate,resultJson,isEffective,loadFrom)
        spark.sql(sql)
    except Exception,e:
        logger.logger.error(e)


#获取当天错误数据list
def selectFromErrorTable():
    try:
        now = datetime.datetime.now()
        tomorrowdate = now + datetime.timedelta(days=1)
        tomorrowtime = int(time.mktime(tomorrowdate.date().timetuple()))
        zerotime = int(time.mktime(now.date().timetuple()))
        spark.sql("use dl_sqds")
        sql = """select 
            check_result
            from dl_sqds.ta_sqds_notice_result
            where unix_timestamp(src_create_date,'yyyy-MM-dd HH:mm:ss') 
            between %s and %s
            """%(zerotime,tomorrowtime)
        df = spark.sql(sql)
        resultlist = []
        for row in df.collect():
            if row["hasSuccess"] is not True:
                resultlist.append(row['errorResults'])
        return resultlist
    except Exception, e:
        logger.logger.error(e)



# if __name__ == "__main__":
#     print (selectCountFromResultTable())



