-- Create Transactions_Data database if not exists
CREATE DATABASE IF NOT EXISTS Transactions_Data;
USE Transactions_Data;

-- Create Transactions table
CREATE TABLE IF NOT EXISTS Transactions (
  eid INT,
  ename STRING,
  age INT,
  jobtype STRING,
  storeid INT,
  storelocation STRING,
  salary BIGINT,
  yrsofexp INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/Transactions_Data.db/Transactions';

