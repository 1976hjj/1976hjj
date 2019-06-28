
# -*-coding:utf-8 -*-
# import ConfigParser
#import sys
#import datetime
#from pyspark.sql import SparkSession
#from pyspark.sql.types import StructType, StructField, StringType
#import os


# import logger
# from defFive import util
from etlinit import *
from etlinit import sc
os.environ['PYSPARK_PYTHON']='/opt/cloudera/parcels/Anaconda-4.1.1/bin/python'


reload(sys)
sys.setdefaultencoding('utf8')


def test():
      s1.sql("use dl_sqds")
      sql = """
                   select 
                  supplier_name,
                  part_name,
                  part_number,
                  key_feature,
                  vehicle_name,
                  update_time,
                  sample_batch,
                  median_value,
                  normal_value,
                  median_name_value
              from dl_sqds.tg_tolerance
              where supplier_name='SMTS' and part_name='C1XX冷凝器' and key_feature='波高'
              group by   supplier_name,
                  part_name,
                  part_number,
                  key_feature,
                  vehicle_name,
                  update_time,
                  sample_batch,
                  median_value,
                  normal_value,
                  median_name_value
              order by sample_batch
              """
      tt_measure = sc.sql(sql)
    # 用于判断结果是否为空，为空则证明数据正确，没有发现错误hasSuccess为true，不为空则表明有数据错误
      tt_measure.show()
test()


