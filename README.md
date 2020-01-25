### 搜集 HIVE表 元数据

---

Hbase表结构:
```bash
表: hive_metadata_collect
row_key: action_id__hive_table

列族: computing
列: application_id的 map数量 reduce数量

列族: scheduler
列: 调度名、开始时间、结束时间、action_id、所属用户

列族: quality
列: 精准度、自定义监控值

列族: store
文件占用大小、文件量
```

下面是例子
```bash
pcsjob@center4: hbase shell
hbase(main):000:0* get 'hive_metadata_collect','202001240001000468__db_name.table_name'
COLUMN                                       CELL
 quality:point_pay_average                   timestamp=1579849213597, value=1
 quality:point_pay_count                     timestamp=1579849213597, value=2
 quality:point_pay_max                       timestamp=1579849213597, value=3
 quality:voucher_buy_price_average           timestamp=1579849213597, value=4
 quality:voucher_buy_price_count             timestamp=1579849213597, value=5
 quality:voucher_buy_price_max               timestamp=1579849213597, value=6
```

---
### 主流程

#### 一、通过zeus_action_history得到最近10分钟～5分钟完成的action_id，获取调度相关元数据

#### 二、通过zeus action log 获取 yarn_job_id，从 Dr-Elephant后台数据里找到 计算资源元数据。

#### 三、通过HIVE表名，找到存储元数据

#### 四、通过调用Apache Griffin数据质量工具的API接口，调用执行检测数据质量的脚本，将调用过的接口参数写入mysql

#### 五、判断mysql中 是否有已过半小时的的数据质量的脚本任务，如果有，获取其检测结果，写入数据质量元数据

##### PS. 为啥不直接获取最近5分钟完成的action_id，因为给 Dr-Elephant 5分钟缓冲时间处理 计算资源元数据入库
---


##### 数据质量-Appache Griffin-调用接口时间
```sql
# mysql表结构
create table if not exists hive_table_data_quality_collect (
    `id` bigint NOT NULL auto_increment COMMENT '主键ID',
    `metric_name` varchar(255) COMMENT 'Appache Griffin Metric',
    `action_id` bigint(11) COMMENT '调度ID',
    `hive_table_name` varchar(255) COMMENT 'HIVE 表名',
    `insert_timestamp` bigint(11) COMMENT '插入时间戳',
    `status` tinyint not null default '0' COMMENT '状态值(0:未处理 1:已处理)',
    PRIMARY KEY (`id`),
    KEY `insert_time` (`insert_timestamp`) USING BTREE,
    KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='HIVE表-数据质量-Appache Griffin';
```

**入口函数: CollectMain.run()**


