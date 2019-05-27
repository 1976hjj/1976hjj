# -*-coding:utf-8 -*-
from han_test import *

class HdfsOperator:

    def __init__(self):
         pass

#类变量获取路径下所有的文件名
    @classmethod
    def hdfs_ls(cls,FilePath):
        re=subprocess.check_output('hadoop fs -ls %s'%(FilePath), shell=True)
        return re

#查找hdfs所有符合文件名的文件(完整路径)
    @classmethod
    def hdfs_findbyname(cls,Path,matchstr):
        re=subprocess.check_output("hdfs dfs -find %s -name '%s' "%(Path,matchstr), shell=True)
        return re

#移动hdfs上的文件,源文件删除
    @classmethod
    def hdfs_mvfile(cls,oPath,tPath):
      try:
             subprocess.check_output("hadoop fs -mv '%s' '%s'"%(oPath, tPath), shell=True)
      except (BaseException,Exception) as e:
             logging.fatal('hdfs_mvfile方法hdfs移动删除命令错误')
             logging.fatal(e)
  #          logging.fatal(traceback.format_exc())

#hdfs查找文件，参数：查找路径，查找文件
    @classmethod
    def hdfs_findfile(cls, findpath, findtxt):
        try:
            re=subprocess.check_output("hdfs dfs -find '%s' -name '%s'" % (findpath, findtxt), shell=True)
            return re
        except (BaseException, Exception) as e:
            logging.fatal('hdfs_findfile寻找文件方法错误')
            logging.fatal(e)

