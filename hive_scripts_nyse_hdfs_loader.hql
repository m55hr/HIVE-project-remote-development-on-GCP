set hive.exec.dynamic.partition.mode=nonstrict;
create database if not exists nyse_db;
use nyse_db;

create table if not exists nyse_daily
(
    ticker     string,
    tradedate  int,
    openprice  float,
    highprice  float,
    lowprice   float,
    closeprice float,
    volume     bigint
)
    partitioned by (trademonth int) stored as parquet;

create table if not exists nyse_stg
(
    ticker     string,
    tradedate  int,
    openprice  float,
    highprice  float,
    lowprice   float,
    closeprice float,
    volume     bigint
)
    row format delimited fields terminated by ',';

load data inpath '/user/${username}/data/nyse'
    overwrite
    into table nyse_stg;

INSERT OVERWRITE TABLE nyse_daily PARTITION (trademonth)
SELECT ns.*,
       SUBSTR(ns.tradedate, 1, 6) AS trademonth
FROM nyse_stg AS ns;

show partitions nyse_daily;

select trademonth, count(*)
from nyse_daily
group by trademonth
order by trademonth;