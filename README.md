flink-connector-clickhouse

flink版本1.12

支持批量写入定时刷新

支持写入集群表和本地表

支持三种写入策略(hash | shuffle | balanced)

暂支持sink，后续加上source

使用：

    CREATE TABLE ck_sink ( 
        name VARCHAR,     
        grade BIGINT, 
        rate FLOAT, 
        more VARCHAR 
    ) WITH (
        'connector' = 'clickhouse',   
        'url' = 'clickhouse://192.168.8.94:8123', /* 如果是写入本地表,则会根据表名从system库获取本地表名称 */
        'username' = '',
        'password' = '',
        'database-name' = 'glab',        /* ClickHouse 数据库名，默认为 default */
        'table-name' = 'ck_test',      /* ClickHouse 数据表名 */
        'sink.batch-size' = '1000',         /* batch 大小 */
        'sink.flush-interval' = '1000',     /* flush 时间间隔 */
        'sink.max-retries' = '1',           /* 最大重试次数 */
        'sink.partition-strategy' = 'balanced', /* hash | shuffle | balanced */
        'sink.write-local' = 'true',        /*如果为ture则默认写入本地表,否则写入集群表*/
        'sink.ignore-delete' = 'true'       /* 忽略 DELETE 并视 UPDATE 为 INSERT */
    )

    create table if not exists ck_kafka(
       tname VARCHAR,
       tgrade BIGINT,
       trate FLOAT,
       tmore VARCHAR
    ")WITH(
        'connector' = 'kafka',
        'topic' = 'ck_test',
        'scan.startup.mode' = 'latest-offset',
        'properties.group.id' = 'ck_test1',
        'properties.bootstrap.servers' = 'ga-kafka1:9092,ga-kafka2:9092',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true',
        'csv.field-delimiter' = '|',
        'csv.null-literal' = ''
    )

    insert into ck_sink select * from ck_kafka



---------------------------------------------------
add by welee.li

原connector不支持clickhouse数据的更新或者删除，故对connector进行了一些修改使其支持数据更新和删除(以插入标识替代删除)

1.配置'sink.ignore-delete' = 'true'时，忽略 DELETE 并视 UPDATE 为 INSERT，与原connector功能一致

2.配置'sink.ignore-delete'='false'时, 对delete的数据增加删除标识并insert到clickhouse中
    实现原理：clickhouse使用 "Engine= ReplacingMergeTree(version) order by pk_id" 表，并在最后两个字段使用sign(数据是否有效标识,1有效,-1无效),version(数据版本号,默认使用插入时间戳)
    1)数据新增：插入sign=1,version=UNIX_TIMESTAMP()数据
    2)数据修改：插入sign=1,version=UNIX_TIMESTAMP()数据
    3)数据删除：插入sign=-1,version=UNIX_TIMESTAMP()数据
    clickhouse ReplacingMergeTree 会自动根据pk_id(主键或唯一标识) version的大小合并记录
    
    局限性：source端数据源必须有主键或数据唯一键标识


使用示例：

mysql表：

    CREATE TABLE `Course` (
    	`c_id` VARCHAR(20) NOT NULL COLLATE 'utf8mb4_general_ci',
    	`c_name` VARCHAR(20) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',
    	`t_id` VARCHAR(20) NOT NULL COLLATE 'utf8mb4_general_ci',
    	PRIMARY KEY (`c_id`) USING BTREE
    )
    COLLATE='utf8mb4_general_ci'
    ENGINE=InnoDB
    ;


clickhouse 表：

    drop table course;
    create table course (
    c_id String,
        c_name String,
        t_id String,
        sign Int8,
        ver UInt64
    )Engine= ReplacingMergeTree(ver)
    order by c_id


flink sql 使用：

    CREATE TABLE flink_test_source_course (
        c_id VARCHAR,
        c_name VARCHAR,
        t_id VARCHAR
    ) WITH (
    'connector' = 'mysql-cdc'
    ,'hostname' = 'xx.xx.xx.xx'
    ,'port' = '3306'
    ,'username' = 'root'
    ,'password' = 'xxxx'
    ,'database-name' = 'xxxx'
    ,'table-name' = 'Course'
    );
    
    
    CREATE TABLE flink_test_sink_course (
        c_id VARCHAR,
        c_name VARCHAR,
        t_id VARCHAR,
        sign TINYINT,
        ver BIGINT,
        PRIMARY KEY (c_id) NOT ENFORCED
    ) WITH (
    'connector' = 'clickhouse',
        'url' = 'clickhouse://xx.xx.xx.xx:8123'
        ,'username' = 'default'
        ,'password' = 'default'
        ,'database-name' = 'xxxx'
        ,'table-name' = 'course'
        ,'sink.write-local' = 'true'
        ,'sink.batch-size' = '1'
        ,'sink.flush-interval' = '1000'
        ,'sink.max-retries' = '3'
    //  ,'sink.partition-strategy' = 'hash'
    //  ,'sink.partition-key' = 'c_id'
        ,'sink.ignore-delete' = 'false'
    );
    
    insert into flink_test_sink_course select c_id,c_name,t_id,cast(1 as TINYINT) as sign, cast(UNIX_TIMESTAMP() as BIGINT) as ver from flink_test_source_course;




