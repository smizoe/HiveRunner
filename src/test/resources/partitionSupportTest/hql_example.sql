CREATE EXTERNAL TABLE ${hiveconf:table.name} (s1 string, s2 string, s3 string)
    PARTITIONED BY (
        year int,
        month int)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE

LOCATION '${hiveconf:HDFS_ROOT_FOO}/foo/';




