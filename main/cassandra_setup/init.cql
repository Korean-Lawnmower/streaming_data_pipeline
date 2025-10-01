CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ecommerce;

CREATE TABLE IF NOT EXISTS logs_by_ip (
    accessed_date timestamp,
    duration_secs int,
    network_protocol text,
    ip text,
    bytes bigint,
    accessed_from text,
    age int,
    gender text,
    country text,
    membership text,
    language text,
    sales double,
    returned text,
    returned_amount double,
    pay_method text,
    item_category text,
    accessed_at timeuuid,
    log_date date,
    error_log text,
    PRIMARY KEY (ip, accessed_at)
) WITH CLUSTERING ORDER BY (accessed_at DESC);

CREATE TABLE IF NOT EXISTS logs_by_day (
    accessed_date timestamp,
    duration_secs int,
    network_protocol text,
    ip text,
    bytes bigint,
    accessed_from text,
    age int,
    gender text,
    country text,
    membership text,
    language text,
    sales double,
    returned text,
    returned_amount double,
    pay_method text,
    item_category text,
    accessed_at timeuuid,
    log_date date,
    error_log text,
    PRIMARY KEY (log_date, accessed_at)
) WITH CLUSTERING ORDER BY (accessed_at ASC);

CREATE TABLE IF NOT EXISTS logs_by_country (
    accessed_date timestamp,
    duration_secs int,
    network_protocol text,
    ip text,
    bytes bigint,
    accessed_from text,
    age int,
    gender text,
    country text,
    membership text,
    language text,
    sales double,
    returned text,
    returned_amount double,
    pay_method text,
    item_category text,
    accessed_at timeuuid,
    log_date date,
    error_log text,
    PRIMARY KEY (country, accessed_at)
) WITH CLUSTERING ORDER BY (accessed_at DESC);

CREATE TABLE IF NOT EXISTS logs_by_category (
    accessed_date timestamp,
    duration_secs int,
    network_protocol text,
    ip text,
    bytes bigint,
    accessed_from text,
    age int,
    gender text,
    country text,
    membership text,
    language text,
    sales double,
    returned text,
    returned_amount double,
    pay_method text,
    item_category text,
    accessed_at timeuuid,
    log_date date,
    error_log text,
    PRIMARY KEY (item_category, accessed_at)
) WITH CLUSTERING ORDER BY (accessed_at DESC);
