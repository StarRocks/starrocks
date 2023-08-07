CREATE MATERIALIZED VIEW lineorder_flat_mv
DISTRIBUTED BY HASH(LO_ORDERDATE, LO_ORDERKEY) BUCKETS 48
REFRESH manual
PROPERTIES (
    "replication_num" = "1"
)
AS SELECT
       l.LO_ORDERKEY AS LO_ORDERKEY,
       l.LO_LINENUMBER AS LO_LINENUMBER,
       l.LO_CUSTKEY AS LO_CUSTKEY,
       l.LO_PARTKEY AS LO_PARTKEY,
       l.LO_SUPPKEY AS LO_SUPPKEY,
       l.LO_ORDERDATE AS LO_ORDERDATE,
       l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
       l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
       l.LO_QUANTITY AS LO_QUANTITY,
       l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
       l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
       l.LO_DISCOUNT AS LO_DISCOUNT,
       l.LO_REVENUE AS LO_REVENUE,
       l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
       l.LO_TAX AS LO_TAX,
       l.LO_COMMITDATE AS LO_COMMITDATE,
       l.LO_SHIPMODE AS LO_SHIPMODE,
       c.C_NAME AS C_NAME,
       c.C_ADDRESS AS C_ADDRESS,
       c.C_CITY AS C_CITY,
       c.C_NATION AS C_NATION,
       c.C_REGION AS C_REGION,
       c.C_PHONE AS C_PHONE,
       c.C_MKTSEGMENT AS C_MKTSEGMENT,
       s.S_NAME AS S_NAME,
       s.S_ADDRESS AS S_ADDRESS,
       s.S_CITY AS S_CITY,
       s.S_NATION AS S_NATION,
       s.S_REGION AS S_REGION,
       s.S_PHONE AS S_PHONE,
       p.P_NAME AS P_NAME,
       p.P_MFGR AS P_MFGR,
       p.P_CATEGORY AS P_CATEGORY,
       p.P_BRAND AS P_BRAND,
       p.P_COLOR AS P_COLOR,
       p.P_TYPE AS P_TYPE,
       p.P_SIZE AS P_SIZE,
       p.P_CONTAINER AS P_CONTAINER,
       d.d_date AS d_date,
       d.d_dayofweek AS d_dayofweek,
       d.d_month AS d_month,
       d.d_year AS d_year,
       d.d_yearmonthnum AS d_yearmonthnum,
       d.d_yearmonth AS d_yearmonth,
       d.d_daynuminweek AS d_daynuminweek,
       d.d_daynuminmonth AS d_daynuminmonth,
       d.d_daynuminyear AS d_daynuminyear,
       d.d_monthnuminyear AS d_monthnuminyear,
       d.d_weeknuminyear AS d_weeknuminyear,
       d.d_sellingseason AS d_sellingseason,
       d.d_lastdayinweekfl AS d_lastdayinweekfl,
       d.d_lastdayinmonthfl AS d_lastdayinmonthfl,
       d.d_holidayfl AS d_holidayfl,
       d.d_weekdayfl AS d_weekdayfl
   FROM lineorder AS l
            INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
            INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
            INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY
            INNER JOIN dates AS d ON l.lo_orderdate = d.d_datekey;
