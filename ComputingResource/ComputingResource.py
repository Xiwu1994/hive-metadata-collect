# coding:utf-8
import torndb
import re
import json
import requests
import sys
import time
sys.path.append("../")
from util.LogUtil import LogUtil

dr_elephant_client = torndb.Connection("host:port", "db", user="user",
                                       password="pass", time_zone='+8:00')
log = LogUtil("ComputingResource")


class ComputingResource(object):
    @staticmethod
    def get_hive_table_name(hive_sql):
        hive_table_name = None
        p1 = re.compile(r'insert\s+overwrite\s+table\s+([^\s]+)')
        p2 = re.compile(r'insert\s+into\s+table\s+([^\s]+)')
        p3 = re.compile(r'create table\s+([^\s]+)\s+as')

        m1 = p1.findall(hive_sql)
        if len(m1) > 0:
            hive_table_name = m1[0]
            return hive_table_name

        m2 = p2.findall(hive_sql)
        if len(m2) > 0:
            hive_table_name = m2[0]
            return hive_table_name

        m3 = p3.findall(hive_sql)
        if len(m3) > 0:
            hive_table_name = m3[0]
            return hive_table_name

        return hive_table_name

    @staticmethod
    def get_hive_sql_by_yarn_job_configuration_info(yarn_job_id):
        job_conf_url = "http://center4.secoo-inc.com:19888/ws/v1/history/mapreduce/jobs/%s/conf" % (yarn_job_id)
        job_conf_list = json.loads(requests.get(job_conf_url).content)['conf']['property']

        hive_sql = None
        for elem in job_conf_list:
            if elem["name"] == "hive.query.string":
                hive_sql = elem["value"]
                break
        return hive_sql

    @staticmethod
    def get_job_basic_info(yarn_job_id, application_id):
        # HSQL、开始和结束时间
        run_sql = """
        select
          job_def_id as hsql,
          start_time,
          finish_time
        from yarn_app_result
        where id = '%s'
        """ % application_id

        start_time, finish_time, hive_table_name = None, None, None
        query_result_list = dr_elephant_client.query(run_sql)
        if len(query_result_list) == 0:
            log.warning("Dr-Elephant 没有对应的 appliction_id: %s" % application_id)
        else:
            query_result = query_result_list[0]
            hsql = query_result['hsql']
            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(query_result['start_time'] / 1000))
            finish_time =time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(query_result['finish_time'] / 1000))
            hive_table_name = ComputingResource.get_hive_table_name(hsql)
            if hive_table_name is None:
                hive_sql = ComputingResource.get_hive_sql_by_yarn_job_configuration_info(yarn_job_id)
                hive_table_name = ComputingResource.get_hive_table_name(hive_sql)
                if hive_table_name is None:
                    log.warning("appliction_id: %s 没有对应的 HIVE TABLE" % application_id)
        return start_time, finish_time, hive_table_name

    @staticmethod
    def get_abnormal_index(application_id):
        # 异常指标
        run_sql = """
        select
          heuristic_name
        from yarn_app_heuristic_result
        where yarn_app_result_id = '%s'
          and score >= 3
        """ % application_id

        abnormal_index = list()
        query_result_list = dr_elephant_client.query(run_sql)
        if len(query_result_list) > 0:
            for query_result in query_result_list:
                heuristic_name = query_result['heuristic_name']
                abnormal_index.append(heuristic_name)
        return abnormal_index

    @staticmethod
    def get_map_reduce_tasks_num(application_id):
        # Map、Reduce Task数量
        run_sql = """
        select
          t1.heuristic_name,
          t2.value
        from yarn_app_heuristic_result t1
        inner join yarn_app_heuristic_result_details t2
        on t1.id = t2.yarn_app_heuristic_result_id and t2.name = 'Number of tasks'
        where t1.yarn_app_result_id = '%s'
          and t1.heuristic_name in ('Mapper Memory', 'Reducer Memory')
        """ % application_id

        mapper_tasks_num, reduce_tasks_num = None, None
        query_result_list = dr_elephant_client.query(run_sql)
        if len(query_result_list) > 0:
            for query_result in query_result_list:
                heuristic_name = query_result['heuristic_name']
                if heuristic_name == "Mapper Memory":
                    mapper_tasks_num = query_result['value']
                elif heuristic_name == "Reducer Memory":
                    reduce_tasks_num = query_result['value']
        return mapper_tasks_num, reduce_tasks_num

    @staticmethod
    def process_yarn_job(yarn_job_id):
        application_id = "application_" + yarn_job_id.split('_', 1)[1]
        # YARN JOB 开始时间、结束时间、HIVE表名
        start_time, finish_time, hive_table_name = ComputingResource.get_job_basic_info(yarn_job_id, application_id)
        if start_time is None:
            return None

        # 异常指标列表 (比如 Map资源使用过高、数据倾斜、GC频繁）
        abnormal_index = ComputingResource.get_abnormal_index(application_id)
        # Map 和 Reduce Task数量
        mapper_tasks_num, reduce_tasks_num = ComputingResource.get_map_reduce_tasks_num(application_id)

        log.info("yarn_job_id: %s start_time: %s, finish_time: %s, hive_table_name: %s, abnormal_index: %s, "
                 "mapper_tasks_num: %s, reduce_tasks_num: %s" % (yarn_job_id, start_time, finish_time, hive_table_name,
                 abnormal_index, mapper_tasks_num, reduce_tasks_num))
        return start_time, finish_time, hive_table_name, abnormal_index, mapper_tasks_num, reduce_tasks_num


if __name__ == "__main__":
    ComputingResource.process_yarn_job('job_1579028891377_4845')
