use ecommerce_dw;

CREATE TABLE IF NOT EXISTS ads_data_quality_report_prod (
    check_date      string comment '业务日期',
    table_name      string comment '被校验表',
    check_type      string comment '校验类型',
    check_name      string comment '校验名称',
    actual_value    string comment '实际值',
    expected_value  string comment '期望值',
    diff_value      string comment '差异值',
    check_result    string comment 'PASS/FAIL',
    error_count     bigint comment '异常数量',
    severity        string comment 'INFO/WARN/ERROR',
    create_time     string comment '写入时间'
)
COMMENT '数据质量监控结果表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
