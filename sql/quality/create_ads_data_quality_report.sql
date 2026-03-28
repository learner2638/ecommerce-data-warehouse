USE ecommerce_dw;

CREATE TABLE IF NOT EXISTS ads_data_quality_report_prod (
    check_date      STRING COMMENT '业务日期',
    table_name      STRING COMMENT '表名',
    check_type      STRING COMMENT '校验类型',
    check_name      STRING COMMENT '校验名称',
    actual_value    STRING COMMENT '实际值',
    expected_value  STRING COMMENT '期望值',
    diff_value      STRING COMMENT '差异值',
    check_result    STRING COMMENT 'PASS/FAIL',
    error_count     BIGINT COMMENT '异常数量',
    severity        STRING COMMENT 'INFO/WARN/ERROR',
    create_time     STRING COMMENT '创建时间'
)
COMMENT '数据质量结果表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
