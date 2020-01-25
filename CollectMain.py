# coding:utf-8
from SchedulerData.SchedulerData import SchedulerData
from ComputingResource.ComputingResource import ComputingResource
from StoreData.StoreData import StoreData
from DataQuality.DataQuality import DataQuality
from util.LogUtil import LogUtil
from util.HbaseUtil import HbaseUtil

log = LogUtil("CollectMain")


class CollectMain(object):
    @staticmethod
    def write_into_hbase(hive_table_info_dict):
        for hive_table_name in hive_table_info_dict:
            action_id = hive_table_info_dict[hive_table_name]["scheduler"]["action_id"]
            row_key = b"%s__%s" % (action_id, hive_table_name)
            scheduler_dict = hive_table_info_dict[hive_table_name]["scheduler"]
            store_dict = hive_table_info_dict[hive_table_name]["store"]
            computing_dict = hive_table_info_dict[hive_table_name]["computing"]

            data = {
                b'scheduler:action_id': b'%s' % scheduler_dict["action_id"],
                b'scheduler:job_name': b'%s' % scheduler_dict["job_name"],
                b'scheduler:start_time': b'%s' % scheduler_dict["start_time"],
                b'scheduler:end_time': b'%s' % scheduler_dict["end_time"],
                b'scheduler:owner': b'%s' % scheduler_dict["owner"],
                b'store:length': b'%s' % store_dict["length"],
                b'store:directory_count': b'%s' % store_dict["directory_count"],
                b'store:file_count': b'%s' % store_dict["file_count"]
            }
            HbaseUtil.put(row_key, data)
            log.info("insert into hbase.. row_key: %s, data: %s" % (row_key, data))

            for application_id in computing_dict:
                data = {
                    b'computing:start_time__%s' % application_id: b'%s' % computing_dict[application_id]["start_time"],
                    b'computing:end_time__%s' % application_id: b'%s' % computing_dict[application_id]["end_time"],
                    b'computing:abnormal_index__%s' % application_id: b'%s' % computing_dict[application_id]["abnormal_index"],
                    b'computing:mapper_tasks_num__%s' % application_id: b'%s' % computing_dict[application_id]["mapper_tasks_num"],
                    b'computing:reduce_tasks_num__%s' % application_id: b'%s' % computing_dict[application_id]["reduce_tasks_num"],
                }
                HbaseUtil.put(row_key, data)
                log.info("insert into hbase.. row_key: %s, data: %s" % (row_key, data))

    @staticmethod
    def collect_data_quality():
        hive_table_data_quality_dict = DataQuality.get_metric_value()
        for action_id_hive_table_name in hive_table_data_quality_dict:
            metric_value_dict = hive_table_data_quality_dict[action_id_hive_table_name]
            row_key = b"%s" % action_id_hive_table_name
            data = {}
            for metric_key in metric_value_dict:
                metric_value = metric_value_dict[metric_key]
                data[b'quality:%s' % metric_key] = b'%s' % metric_value
            HbaseUtil.put(row_key, data)
            log.info("insert into hbase.. row_key: %s, data: %s" % (row_key, data))

    @staticmethod
    def run():
        # 一、获取最近 最近10分钟～5分钟完成的action_id，和对应的调度元数据
        action_info_list = SchedulerData.run()
        hive_table_info_dict = dict()
        for action_info in action_info_list:
            action_id, start_time, end_time, owner, job_name, yarn_job_id_list = action_info

            # 二、计算资源元数据 (通过yarn_job_id获取)
            for yarn_job_id in yarn_job_id_list:
                yarn_job_computing_info_list = ComputingResource.process_yarn_job(yarn_job_id)

                yarn_start_time, yarn_finish_time, hive_table_name, abnormal_index, mapper_tasks_num, \
                    reduce_tasks_num = None, None, None, None, None, None
                if yarn_job_computing_info_list is not None:
                    yarn_start_time, yarn_finish_time, hive_table_name, abnormal_index, mapper_tasks_num, \
                        reduce_tasks_num = yarn_job_computing_info_list

                if hive_table_name is not None:
                    hive_table_info_dict.setdefault(hive_table_name, {
                        "scheduler": {
                            "action_id": action_id,
                            "job_name": job_name,
                            "start_time": start_time,
                            "end_time": end_time,
                            "owner": owner
                        },
                        "computing": dict(),
                        "quality": dict(),
                        "store": dict()
                    })

                    hive_table_info_dict[hive_table_name]["computing"][yarn_job_id] = {
                        "start_time": yarn_start_time,
                        "end_time": yarn_finish_time,
                        "abnormal_index": abnormal_index,
                        "mapper_tasks_num": mapper_tasks_num,
                        "reduce_tasks_num": reduce_tasks_num
                    }
                else:
                    log.info("job_name: %s action_id: %s yarn_job_id: %s 没有HIVE表" % (job_name, action_id, yarn_job_id))

        # 三、存储元数据 & 数据质量元数据
        for hive_table_name in hive_table_info_dict.keys():
            if len(hive_table_name.split('.')) > 1:
                # 开始计算数据质量
                action_id = hive_table_info_dict[hive_table_name]["scheduler"]["action_id"]
                DataQuality.trigger_job_by_hive_table_name(hive_table_name, action_id)

                # 获取存储元数据
                hive_db, hive_table = hive_table_name.split('.')
                length, directory_count, file_count = StoreData.get_table_store_info(hive_db, hive_table)
                hive_table_info_dict[hive_table_name]["store"] = {
                    # 单位 MB
                    "length": length,
                    "directory_count": directory_count,
                    "file_count": file_count
                }
            else:
                log.error("HIVE表: %s 没有对应的库名" % hive_table_name)

        CollectMain.write_into_hbase(hive_table_info_dict)

        # 四、获取数据质量元数据（半小时前的历史表）
        CollectMain.collect_data_quality()


if __name__ == "__main__":
    CollectMain.run()

