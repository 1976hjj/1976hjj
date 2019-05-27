from  HdfsOperator import *
from han_test import *
from han_test import sc

reload(sys)
sys.setdefaultencoding('utf8')

print(sc)
csvdf = sc.read.csv('/user/hive/warehouse_ext/DL/sqds/rawdata/tm_measure_data/smts-constantspeeddriveshaft-measureddate-20190524.csv', encoding='GBK', header='true')
csvdf.registerTempTable('csvdf')
matcheddata = sc.sql('select supplier_name from csvdf')
for i in matcheddata.collect():
    matcheddata=i[0]
    print(matcheddata)
if matcheddata=='SMTS':
    print('True')


#ls_list = subprocess.call(["ls","/home"])
#re=subprocess.check_output('hadoop fs -ls %s'%('/user/hive/warehouse_ext/DL/'),shell=True)

#HdfsOperator.hdfs_ls('/user/hive/warehouse_ext/DL/')
#HdfsOperator.hdfs_findbyname('/user/hive/warehouse_ext/DL/sqds','*.csv')
#HdfsOperator.hdfs_mvfile('/user/hive/warehouse_ext/DL/sqds/1.csv','/user/hive/warehouse_ext/DL/sqds/test')
#print(HdfsOperator.hdfs_findfile('/user/hive/warehouse_ext/DL/sqds/','*.csv'))

