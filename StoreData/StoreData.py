# coding:utf-8
import torndb
import pyhdfs
import sys
import traceback
sys.path.append("../")
from util.LogUtil import LogUtil

hive_host=""
hive_port=3306
hive_database=""
hive_user=""
hive_pass=""

hive_metadata_client = torndb.Connection("%s:%s" %(hive_host,hive_port), hive_database, user=hive_user,
                                         password=hive_pass, time_zone='+8:00')
hdfs_client = pyhdfs.HdfsClient("hdfs_host:50070")
log = LogUtil("StoreData")


class StoreData(object):
    @staticmethod
    def get_hdfs_location(hive_db_name, hive_table_name):
        location = None
        sql = """
        select
          LOCATION
        from SDS
        where SD_ID = (
          select
            SD_ID
          from TBLS
          where DB_ID = (select DB_ID from DBS where NAME = '%s')
            and TBL_NAME = '%s')
        """ % (hive_db_name, hive_table_name)
        query_result_list = hive_metadata_client.query(sql)
        if len(query_result_list) == 0:
            log.error("%s.%s 没有找到对应的 HDFS LOCATION" % (hive_db_name, hive_table_name))
        else:
            location = query_result_list[0]['LOCATION']
        return location

    @staticmethod
    def get_hdfs_size(location):
        length, directory_count, file_count = None, None, None
        try:
            if "tesla-cluster" in location:
                location = location.split("tesla-cluster")[1]
            summary_info_dict = hdfs_client.get_content_summary(location)

            if "length" in summary_info_dict:
                # 单位 MB
                length = round(summary_info_dict["length"] * 1.0 / 1024/1024, 3)
            if "directoryCount" in summary_info_dict:
                directory_count = summary_info_dict["directoryCount"]
            if "fileCount" in summary_info_dict:
                file_count = summary_info_dict["fileCount"]
        except Exception, e:
            traceback.print_exc()
            log.error("location: %s 获取存储元数据失败 报错信息: %s" % (location, e))
        finally:
            return length, directory_count, file_count

    @staticmethod
    def get_table_store_info(hive_db_name, hive_table_name):
        location = StoreData.get_hdfs_location(hive_db_name, hive_table_name)
        # 占用HDFS空间（MB）、目录数量（分区）、文件数量
        length, directory_count, file_count = StoreData.get_hdfs_size(location)
        log.info("HIVE表: %s.%s length: %s, directoryCount: %s, fileCount: %s" %
                 (hive_db_name, hive_table_name, length, directory_count, file_count))
        return length, directory_count, file_count


if __name__ == "__main__":
    print 0
