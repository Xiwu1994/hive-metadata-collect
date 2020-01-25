# coding:utf-8
import torndb
import re
import time
import sys
sys.path.append("../")
from util.LogUtil import LogUtil
reload(sys)
sys.setdefaultencoding('utf8')

zeus_client = torndb.Connection("host:port", "db", user="user", password="pass", time_zone='+8:00')
log = LogUtil("SchedulerData")


class SchedulerData(object):
    @staticmethod
    def get_action_yarn_job_id_list(action_log):
        """
        Zeus调度log里 找到 对应的 Yarn Job Id
        :return: yarn job_id list
        """
        yarn_job_id_list = list()
        p = re.compile(r'Ended Job = ([^\n]*)\n')
        for job_id in p.findall(action_log):
            yarn_job_id_list.append(job_id)
        return yarn_job_id_list

    @staticmethod
    def get_latest_success_action_info():
        before_5_min_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time()) - 5 * 60))
        before_10_min_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time()) - 10 * 60))
        run_sql = """
          select
            t1.action_id as action_id,
            t1.log as log,
            t1.start_time as start_time,
            t1.end_time as end_time,
            t2.owner as owner,
            t2.name as job_name
          from (
              select
                action_id, log, start_time, end_time, job_id
              from zeus_action_history
              where gmt_modified >= '%s' and gmt_modified <= '%s'
                and status = 'success'
          ) t1
          inner join zeus_job t2
          on t1.job_id = t2.id
        """ % (before_10_min_datetime, before_5_min_datetime)
        query_list = zeus_client.query(run_sql)
        log.info("(%s ~ %s) 跑完 (%s) 个调度任务" % (before_10_min_datetime, before_5_min_datetime, len(query_list)))
        return query_list

    @staticmethod
    def process_action_info(action_info_dict):
        action_id = str(action_info_dict['action_id'])
        log = action_info_dict['log']
        start_time = str(action_info_dict['start_time'])
        end_time = str(action_info_dict['end_time'])
        owner = action_info_dict['owner']
        job_name = action_info_dict['job_name']
        yarn_job_id_list = SchedulerData.get_action_yarn_job_id_list(log)
        return action_id, start_time, end_time, owner, job_name, yarn_job_id_list

    @staticmethod
    def run():
        query_list = SchedulerData.get_latest_success_action_info()
        action_info_list = []
        for action_info_dict in query_list:
            # 调度批次号、开始时间、结束时间、任务所属人、任务名、对应的YARN JOB列表
            action_id, start_time, end_time, owner, job_name, yarn_job_id_list = SchedulerData.process_action_info(action_info_dict)
            action_info_list.append((action_id, start_time, end_time, owner, job_name, yarn_job_id_list))
            log.info("已完成的调度任务元数据信息 action_id: %s, start_time: %s, end_time: %s, owner: %s, job_name: %s, "
                     "yarn_job_id_list: %s" % (action_id, start_time, end_time, owner, job_name, yarn_job_id_list))
        return action_info_list


if __name__ == "__main__":
    SchedulerData.run()
