# coding:utf-8
import requests
import json
import os
import sys
import torndb
import time
import ConfigParser
sys.path.append("../")
from util.LogUtil import LogUtil


# 参考文档 https://github.com/apache/griffin/blob/master/griffin-doc/service/api-guide.md
"""
两个功能：
一、调用ApacheGriffin的接口，触发检查数据质量的JOB，并将触发的时间点、JOB名，写入数据库中
二、检查数据库中的触发任务，是否已经过去半小时（假设半小时数据质量的任务已完成，目前接口没有提供好确切完成状态），将数据质量结果写入Hbase中
"""
log = LogUtil("DataQuality")


class DataQuality(object):
    cf = ConfigParser.ConfigParser()
    cf.read(os.path.abspath(os.path.dirname(__file__)) + "/DataQualityProfile.ini")
    mysql_client = torndb.Connection("host:port", "db",
                           user="user", password="pass", time_zone='+8:00')

    @staticmethod
    def trigger_job_by_hive_table_name(hive_table_name, action_id):
        job_ids = DataQuality.find_job_id_by_hive_table_name(hive_table_name)
        if job_ids is not None:
            for job_id in job_ids.split(','):
                log.info("hive table: %s, start trigger job: %s" % (hive_table_name, job_id))
                trigger_key = DataQuality.trigger_job_by_id(job_id)
                log.info("trgger_key: %s" % trigger_key)
                DataQuality.insert_into_wait_get_value_queue(job_id, hive_table_name, action_id)
        else:
            log.info("hive table:%s doesn't have a trigger job" % hive_table_name)

    @staticmethod
    def get_metric_value(wait_seconds=1800):
        hive_table_data_quality_dict = {}

        before_timestamp = int(time.time()) - wait_seconds
        sql = "select id,metric_name,action_id,hive_table_name " \
              "from hive_table_data_quality_collect where insert_timestamp <= %s and status = 0"
        query_result_list = DataQuality.mysql_client.query(sql, before_timestamp)
        for query_result in query_result_list:
            mysql_id = query_result['id']
            metric_name = query_result['metric_name']
            action_id = query_result['action_id']
            hive_table_name = query_result['hive_table_name']
            log.info("start get hive: %s, action_id: %s, metric_name: %s  data quality"
                     % (hive_table_name, action_id, metric_name))
            metric_value_dict = DataQuality.get_metric_value_by_name(metric_name)
            hive_table_data_quality_dict.setdefault('%s__%s' % (action_id, hive_table_name), metric_value_dict)

            DataQuality.update_mysql_status(mysql_id)

        return hive_table_data_quality_dict

    @staticmethod
    def update_mysql_status(mysql_id):
        sql = "update hive_table_data_quality_collect set status = 1 where id = %s"
        DataQuality.mysql_client.execute(sql, mysql_id)

    @staticmethod
    def get_metric_name_by_job_id(job_id):
        url = "http://bigdatagriffin.company.cn/api/v1/jobs/config?jobId=%s" % job_id
        response = requests.get(url)
        return json.loads(response.content)["metric.name"]

    @staticmethod
    def insert_into_wait_get_value_queue(job_id, hive_table_name, action_id):
        metric_name = DataQuality.get_metric_name_by_job_id(job_id)
        now_timestamp = int(time.time())
        sql = "INSERT INTO hive_table_data_quality_collect " \
              "(metric_name,action_id,hive_table_name,insert_timestamp) VALUES (%s,%s,%s,%s)"
        DataQuality.mysql_client.insert(sql, metric_name, int(action_id), hive_table_name, now_timestamp)
        log.info("insert mysql --- action_id: %s hive_table: %s, metric_name: %s of the job_id: %s, timestamp: %s"
                 % (action_id, hive_table_name, metric_name, job_id, now_timestamp))

    @staticmethod
    def find_job_id_by_hive_table_name(hive_table_name):
        # TODO: 配置文件（先通过本地文件，后续可改成mysql表）
        job_ids = None
        if DataQuality.cf.has_section(hive_table_name):
            job_ids = DataQuality.cf.get(hive_table_name, "trigger_job_ids")
        return job_ids

    @staticmethod
    def trigger_job_by_id(job_id):
        # https://github.com/apache/griffin/blob/master/griffin-doc/service/api-guide.md#trigger-job-by-id
        headers = {"Content-Type": "application/json"}
        url = "http://bigdatagriffin.company.cn/api/v1/jobs/trigger/%s" % job_id
        response = requests.post(url=url, headers=headers).content
        trigger_key = json.loads(response)['triggerKey']
        return trigger_key

    @staticmethod
    def find_job_instance_by_trigger_key(trigger_key):
        """
        参考接口: https://github.com/apache/griffin/blob/master/griffin-doc/service/api-guide.md#find-job-instance-by-triggerkey
        返回: [{"id":11,"sessionId":343,"state":"STARTING","type":"BATCH","predicateGroup":"PG",
        "predicateName":"order_point_and_voucher_predicate_1579697798720",
        "triggerKey":"DEFAULT.6da64b5bd2ee-f601abe1-3dcd-43d8-8c90-eaacc0d9cdb9","timestamp":1579697798721,
        "expireTimestamp":1580302598721}]
        """
        # TODO: 2020.1.22 find_job_instance_by_trigger_key 接口返回status只有starting,并没返回success状态
        # 因此有没有这个接口都无所谓了
        url = "http://bigdatagriffin.company.cn/api/v1/jobs/triggerKeys/%s" % trigger_key
        response_data = requests.get(url).content
        print response_data

    @staticmethod
    def get_metric_value_by_name(metric_name):
        # https://github.com/apache/griffin/blob/master/griffin-doc/service/api-guide.md#get-metric-values-by-name
        # >>> 选择最近的一条数据质量记录
        url = "http://bigdatagriffin.company.cn/api/v1/metrics/values?metricName=%s&size=1" % metric_name
        response_data = requests.get(url).content
        for metric_dict in json.loads(response_data):
            metric_value_dict = metric_dict["value"]
            log.info("metric_name: %s, metric_value: %s" % (metric_name, metric_value_dict))
            return metric_value_dict


if __name__ == "__main__":
    DataQuality.get_metric_value(wait_seconds=-1)
