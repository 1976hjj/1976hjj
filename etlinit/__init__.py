# -*-coding:utf-8 -*-
import subprocess
import ConfigParser
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,DecimalType
import os
import logging
import traceback
import time

__all__=['subprocess','ConfigParser','sys','datetime','SparkSession','StructType', 'StructField', 'StringType','os','traceback','time','logging']
print('init')
logging.basicConfig(level=logging.INFO,
                    filename='/home/sys_sqds/fivetest/log/log.txt',
                   filemode='a',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

sc = SparkSession.builder.master("local[5]") \
    .appName("SQDS") \
    .enableHiveSupport() \
    .getOrCreate()
sc.sparkContext.setLogLevel("WARN")