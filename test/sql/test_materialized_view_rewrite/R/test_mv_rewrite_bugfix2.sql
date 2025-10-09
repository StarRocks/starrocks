-- name: test_mv_rewrite_bugfix2
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE TABLE `dim_region` (
  `region_id` bigint(20) NULL COMMENT "",
  `region_name` varchar(255) NULL COMMENT "",
  `region_level` varchar(64) NULL COMMENT "",
  `region_order` bigint(20) NULL COMMENT "",
  `country_id` bigint(20) NULL COMMENT "",
  `city_id` bigint(20) NULL COMMENT "",
  `city_name` varchar(255) NULL COMMENT "",
  `region_type` varchar(64) NULL COMMENT "",
  `region_code` varchar(64) NULL COMMENT "",
  `purpose` varchar(64) NULL COMMENT "",
  `parent_country` varchar(255) NULL COMMENT "",
  `parent_territory` varchar(255) NULL COMMENT "",
  `parent_region` varchar(255) NULL COMMENT "",
  `parent_city` varchar(255) NULL COMMENT "",
  `parent_subregion` varchar(255) NULL COMMENT "",
  `parent_district` varchar(255) NULL COMMENT "",
  `parent_area` varchar(255) NULL COMMENT "",
  `geohash_array` array<varchar(64)> NULL COMMENT "",
  `area_id_array` array<int(11)> NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`region_id`, `region_name`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
CREATE TABLE `fact_orders` (
  `city_id` int(11) NULL COMMENT "",
  `country_id` int(11) NULL COMMENT "",
  `group_id` varchar(4) NULL COMMENT "",
  `vehicle_group` varchar(64) NULL COMMENT "",
  `vehicle_type` varchar(64) NULL COMMENT "",
  `wheels` varchar(2) NULL COMMENT "",
  `vehicle_id` bigint(20) NULL COMMENT "",
  `geohash` varchar(64) NULL COMMENT "",
  `base_surge` double NULL COMMENT "",
  `order_code` varchar(64) NULL COMMENT "",
  `total_amount` double NULL COMMENT "",
  `distance_km` double NULL COMMENT "",
  `order_time` datetime NULL COMMENT "",
  `surge` double NULL COMMENT "",
  `total_amount_usd` double NULL COMMENT "",
  `date_id` int(11) NULL COMMENT "",
  `series_id` varchar(64) NULL COMMENT "",
  `order_status` varchar(64) NULL COMMENT "",
  `local_time` datetime NULL COMMENT "",
  `local_date` datetime NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`city_id`, `country_id`, `group_id`)
COMMENT "OLAP"
PARTITION BY RANGE(`date_id`)
(PARTITION p20230601 VALUES [("20230601"), ("20230602")),
PARTITION p20230602 VALUES [("20230602"), ("20230603")),
PARTITION p20230603 VALUES [("20230603"), ("20230604")),
PARTITION p20230604 VALUES [("20230604"), ("20230605")),
PARTITION p20230605 VALUES [("20230605"), ("20230606")),
PARTITION p20230606 VALUES [("20230606"), ("20230607")),
PARTITION p20230607 VALUES [("20230607"), ("20230608")),
PARTITION p20230608 VALUES [("20230608"), ("20230609")),
PARTITION p20230609 VALUES [("20230609"), ("20230610")),
PARTITION p20230610 VALUES [("20230610"), ("20230611")),
PARTITION p20230611 VALUES [("20230611"), ("20230612")),
PARTITION p20230612 VALUES [("20230612"), ("20230613")),
PARTITION p20230613 VALUES [("20230613"), ("20230614")),
PARTITION p20230614 VALUES [("20230614"), ("20230615")),
PARTITION p20230615 VALUES [("20230615"), ("20230616")),
PARTITION p20230616 VALUES [("20230616"), ("20230617")),
PARTITION p20230617 VALUES [("20230617"), ("20230618")),
PARTITION p20230618 VALUES [("20230618"), ("20230619")),
PARTITION p20230619 VALUES [("20230619"), ("20230620")),
PARTITION p20230620 VALUES [("20230620"), ("20230621")),
PARTITION p20230621 VALUES [("20230621"), ("20230622")),
PARTITION p20230622 VALUES [("20230622"), ("20230623")),
PARTITION p20230623 VALUES [("20230623"), ("20230624")),
PARTITION p20230624 VALUES [("20230624"), ("20230625")),
PARTITION p20230625 VALUES [("20230625"), ("20230626")),
PARTITION p20230626 VALUES [("20230626"), ("20230627")),
PARTITION p20230627 VALUES [("20230627"), ("20230628")),
PARTITION p20230628 VALUES [("20230628"), ("20230629")),
PARTITION p20230629 VALUES [("20230629"), ("20230630")),
PARTITION p20230630 VALUES [("20230630"), ("20230701")))
DISTRIBUTED BY HASH(`order_time`) BUCKETS 8 
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO fact_orders VALUES
(6, 4, 'G1', 'Premium', 'Luxury', '4', 16866, 'w21g8', 1.5, 'ORD001', 25.00, 5.0, '2025-08-25 09:15:00', 1.2, 25.00, 20230625, 'S001', 'booked', '2025-08-25 09:00:00', '2025-08-25 00:00:00'),
(6, 4, 'G2', 'Standard', 'Compact', '4', 15829, 'w21g9', 1.0, 'ORD002', 12.50, 4.0, '2025-08-26 14:30:00', 1.0, 12.50, 20230626, 'S002', 'booked', '2025-08-26 14:00:00', '2025-08-26 00:00:00'),
(6, 4, 'G3', 'Premium', 'SUV', '4', 15272, 'w21g7', 1.8, 'ORD003', 30.00, 7.5, '2025-08-27 18:45:00', 1.5, 30.00, 20230627, 'S003', 'booked', '2025-08-27 18:00:00', '2025-08-27 00:00:00'),
(6, 4, 'G4', 'Economy', 'Hatchback', '4', 10495, 'w21g6', 0.8, 'ORD004', 9.60, 3.2, '2025-08-28 11:20:00', 0.9, 9.60, 20230628, 'S004', 'booked', '2025-08-28 11:00:00', '2025-08-28 00:00:00'),
(6, 4, 'G5', 'Motorcycle', 'Bike', '2', 462, 'w21g5', 0.5, 'ORD005', 5.25, 2.1, '2025-08-29 16:10:00', 0.6, 5.25, 20230629, 'S005', 'booked', '2025-08-29 16:00:00', '2025-08-29 00:00:00');
-- result:
-- !result
INSERT INTO dim_region VALUES
    (101, 'Central Subregion', 'Subregion', 1, 1, 6, 'Metro City', 'Urban', 'REG101', 'Generic', 'Country', 'Territory', 'Region', 'City', 'Central Subregion', NULL, NULL, ['w21g8'], NULL),
    (102, 'Downtown District', 'District', 2, 1, 6, 'Metro City', 'Urban', 'REG102', 'Generic', 'Country', 'Territory', 'Region', 'City', NULL, 'Downtown District', NULL, ['w21g9'], NULL),
    (103, 'Market Area', 'Area', 3, 1, 6, 'Metro City', 'Commercial', 'REG103', 'Generic', 'Country', 'Territory', 'Region', 'City', NULL, NULL, 'Market Area', ['w21g7'], NULL),
    (104, 'North Subregion', 'Subregion', 4, 1, 6, 'Metro City', 'Residential', 'REG104', 'Generic', 'Country', 'Territory', 'Region', 'City', 'North Subregion', NULL, NULL, ['w21g6', 'w21g5', 'w21g4'], NULL),
    (105, 'Special Zone', 'Subregion', 5, 1, 6, 'Metro City', 'Special', 'REG105', 'Special', 'Country', 'Territory', 'Region', 'City', 'Special Zone', NULL, NULL, ['w21g3'], NULL),
    (1, 'Downtown', 'City', 1, 1, 101, 'Metropolis', 'Urban', 'REG001', 'Commercial', 'CountryA', 'TerritoryX', 'RegionY', 'Metropolis', 'Central', 'Downtown', 'CBD', ['geohash1', 'geohash2'], [1001, 1002]),
    (2, 'Tech Park', 'District', 2, 1, 101, 'Metropolis', 'Suburban', 'REG002', 'Industrial', 'CountryA', 'TerritoryX', 'RegionY', 'Metropolis', 'East', 'Tech Zone', 'Innovation Park', ['geohash3', 'geohash4'], [2001, 2002]),
    (3, 'Beachside', 'Subregion', 3, 1, 102, 'Coastal City', 'Resort', 'REG003', 'Tourism', 'CountryA', 'TerritoryX', 'RegionZ', 'Coastal City', 'South', 'Beach District', 'Seaside', ['geohash5', 'geohash6'], [3001, 3002]),
    (4, 'Mountain View', 'Region', 4, 1, 103, 'Hill Town', 'Rural', 'REG004', 'Residential', 'CountryA', 'TerritoryY', 'RegionW', 'Hill Town', 'North', 'Highland', 'Scenic Area', ['geohash7', 'geohash8'], [4001, 4002]),
    (5, 'Old Town', 'Area', 5, 2, 201, 'Historic City', 'Heritage', 'REG005', 'Cultural', 'CountryB', 'TerritoryZ', 'RegionV', 'Historic City', 'Center', 'Heritage Zone', 'Ancient Quarter', ['geohash9', 'geohash10'], [5001, 5002]);
-- result:
-- !result
INSERT INTO fact_orders VALUES 
(101, 1, 'G1', 'Standard', 'Sedan', '4', 1001, 'ws8g', 1.2, 'ORD006', 15.50, 5.3, '2023-10-15 08:30:45', 1.1, 15.50, 20230615, 'S006', 'completed', '2023-10-15 08:00:00', '2023-10-15 00:00:00'),
(101, 1, 'G2', 'Premium', 'SUV', '4', 1002, 'ws8h', 1.5, 'ORD007', 25.75, 7.8, '2023-10-15 09:15:22', 1.3, 25.75, 20230615, 'S007', 'completed', '2023-10-15 09:00:00', '2023-10-15 00:00:00'),
(102, 1, 'G3', 'Motorcycle', 'Bike', '2', 2001, 'ws9j', 1.0, 'ORD008', 8.20, 3.1, '2023-10-16 14:45:10', 1.0, 8.20, 20230616, 'S008', 'cancelled', '2023-10-16 14:00:00', '2023-10-16 00:00:00'),
(103, 2, 'G4', 'Delivery', 'Truck', '6', 3001, 'ws7k', 1.8, 'ORD009', 45.90, 12.5, '2023-10-17 11:20:33', 1.6, 45.90, 20230617, 'S009', 'completed', '2023-10-17 11:00:00', '2023-10-17 00:00:00'),
(101, 1, 'G1', 'Pool', 'Compact', '4', 1003, 'ws8m', 0.9, 'ORD010', 10.25, 4.2, '2023-10-18 17:55:18', 0.8, 10.25, 20230618, 'S010', 'completed', '2023-10-18 17:00:00', '2023-10-18 00:00:00');
-- result:
-- !result
CREATE MATERIALIZED VIEW `mv_region_geohash`
DISTRIBUTED BY RANDOM BUCKETS 1
REFRESH ASYNC EVERY(INTERVAL 6 HOUR)
PROPERTIES (
"replication_num" = "1")
AS SELECT `t2`.`geohash`, 
    max(CASE WHEN (`t2`.`region_level` = 'Subregion') THEN `t2`.`region_id` ELSE NULL END) AS `subregion_id`, 
    max(CASE WHEN (`t2`.`region_level` = 'Subregion') THEN `t2`.`region_name` ELSE NULL END) AS `subregion_name`, 
    max(CASE WHEN (`t2`.`region_level` = 'District') THEN `t2`.`region_id` ELSE NULL END) AS `district_id`, 
    max(CASE WHEN (`t2`.`region_level` = 'District') THEN `t2`.`region_name` ELSE NULL END) AS `district_name`, 
    max(CASE WHEN (`t2`.`region_level` = 'Area') THEN `t2`.`region_id` ELSE NULL END) AS `area_id`, 
    max(CASE WHEN (`t2`.`region_level` = 'Area') THEN `t2`.`region_name` ELSE NULL END) AS `area_name`
FROM (
    SELECT `dim_region`.`city_id`, `dim_region`.`region_level`, `dim_region`.`region_id`, `dim_region`.`region_name`, `dim_region`.`purpose`, `t0`.`geohash`
    FROM `dim_region` AS `dim_region` 
    CROSS JOIN LATERAL unnest(`dim_region`.`geohash_array`) t0(`geohash`) 
    WHERE `dim_region`.`region_level` IN ('Subregion', 'District', 'Area')
) `t2`
WHERE `t2`.`purpose` = 'Generic'
GROUP BY `t2`.`geohash`;
-- result:
-- !result
CREATE MATERIALIZED VIEW `mv_fact_orders_1` 
PARTITION BY (`date_id`)
DISTRIBUTED BY HASH(`geohash`)
ORDER BY (vehicle_id)
REFRESH ASYNC
PROPERTIES (
"replication_num" = "1",
"query_rewrite_consistency" = "loose"
)
AS SELECT date_trunc('day', `fact_orders`.`local_time`) AS `local_time_day`, 
    date_trunc('hour', `fact_orders`.`local_time`) AS `local_time_hour`, 
    `fact_orders`.`geohash`, `fact_orders`.`date_id`, `fact_orders`.`city_id`, `fact_orders`.`country_id`, 
    `fact_orders`.`vehicle_id`, `fact_orders`.`wheels`, `fact_orders`.`vehicle_type`, `fact_orders`.`vehicle_group`, 
    `fact_orders`.`order_status`, 
    sum(`fact_orders`.`total_amount_usd`) AS `sum_total_amount_usd`, 
    sum(`fact_orders`.`total_amount`) AS `sum_total_amount`, 
    sum(`fact_orders`.`distance_km`) AS `sum_distance_km`, 
    sum(`fact_orders`.`surge`) AS `sum_surge`, 
    count(`fact_orders`.`surge`) AS `count_surge`
FROM `fact_orders`
WHERE `fact_orders`.`order_code` != ''
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;
-- result:
-- !result
CREATE MATERIALIZED VIEW `mv_fact_orders_2` 
PARTITION BY (`date_id`)
DISTRIBUTED BY HASH(`geohash`)
ORDER BY (country_id)
REFRESH ASYNC
PROPERTIES (
"replication_num" = "1",
"query_rewrite_consistency" = "loose")
AS SELECT date_trunc('day', `fact_orders`.`order_time`) AS `order_time_day`, 
    date_trunc('hour', `fact_orders`.`order_time`) AS `order_time_hour`, 
    date_trunc('hour', `fact_orders`.`local_time`) AS `local_time_hour`, 
    date_trunc('day', `fact_orders`.`local_time`) AS `local_time_day`, 
    `fact_orders`.`geohash`, `fact_orders`.`date_id`, `fact_orders`.`city_id`, `fact_orders`.`country_id`, 
    `fact_orders`.`vehicle_id`, `fact_orders`.`wheels`, `fact_orders`.`vehicle_type`, `fact_orders`.`vehicle_group`, 
    `fact_orders`.`order_status`, 
    sum(`fact_orders`.`total_amount_usd`) AS `sum_total_amount_usd`, 
    sum(`fact_orders`.`total_amount`) AS `sum_total_amount`, 
    sum(`fact_orders`.`distance_km`) AS `sum_distance_km`, 
    sum(`fact_orders`.`surge`) AS `sum_surge`, 
    count(`fact_orders`.`surge`) AS `count_surge`
FROM `fact_orders`
WHERE `fact_orders`.`order_code` = ''
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;
-- result:
-- !result
REFRESH MATERIALIZED VIEW mv_fact_orders_1 FORCE WITH SYNC MODE;
REFRESH MATERIALIZED VIEW mv_fact_orders_2 FORCE WITH SYNC MODE;
REFRESH MATERIALIZED VIEW mv_region_geohash FORCE WITH SYNC MODE;
SELECT subregion_name, CAST(SUM(total_amount) AS DOUBLE) / CAST(SUM(distance_km) AS DOUBLE) AS avg_fare_per_km FROM fact_orders AS f LEFT JOIN mv_region_geohash AS r ON CAST(r.geohash AS varchar)= CAST(f.geohash AS varchar) WHERE DATE_TRUNC('day', local_time) >= DATE_TRUNC('hour', STR_TO_DATE('2025-08-25 00:00:00', '%Y-%m-%d %H:%i:%S')) AND DATE_TRUNC('day', local_time) < DATE_TRUNC('hour', STR_TO_DATE('2025-09-01 00:00:00', '%Y-%m-%d %H:%i:%S')) AND date_id >= 20230623 AND date_id < 20250903 AND (country_id = 4 and city_id in (6) and vehicle_id in (16866, 16860, 16859, 16810, 15829, 15828, 15827, 15826, 15825, 15824, 15817, 15816, 15568, 15272, 15271, 15270, 15269, 15268, 15267, 15266, 15265, 13912, 13904, 13806, 13805, 13804, 13803, 13802, 13801, 13800, 13799, 13456, 13341, 12138, 11770, 11655, 11631, 10742, 10540, 10500, 10499, 10498, 10496, 10495, 10494, 10493, 10492, 10491, 10490, 10489, 10488, 10487, 10486, 10485, 10484, 10479, 9979, 9450, 9338, 8920, 8522, 8183, 7387, 6709, 6705, 4894, 4152, 4131, 3455, 2792, 2742, 2275, 462, 302, 187, 156, 69, 21, 20, 19, 11) and (order_code != '' and order_status = 'booked')) GROUP BY 1 ORDER BY 1 LIMIT 3;
-- result:
None	3.6956521739130435
Central Subregion	5.0
North Subregion	2.8018867924528297
-- !result
function: print_hit_materialized_views("SELECT subregion_name, CAST(SUM(total_amount) AS DOUBLE) / CAST(SUM(distance_km) AS DOUBLE) AS avg_fare_per_km FROM fact_orders AS f LEFT JOIN mv_region_geohash AS r ON CAST(r.geohash AS varchar)= CAST(f.geohash AS varchar) WHERE DATE_TRUNC('day', local_time) >= DATE_TRUNC('hour', STR_TO_DATE('2025-08-25 00:00:00', '%Y-%m-%d %H:%i:%S')) AND DATE_TRUNC('day', local_time) < DATE_TRUNC('hour', STR_TO_DATE('2025-09-01 00:00:00', '%Y-%m-%d %H:%i:%S')) AND date_id >= 20230623 AND date_id < 20250903 AND (country_id = 4 and city_id in (6) and vehicle_id in (16866, 16860, 16859, 16810, 15829, 15828, 15827, 15826, 15825, 15824, 15817, 15816, 15568, 15272, 15271, 15270, 15269, 15268, 15267, 15266, 15265, 13912, 13904, 13806, 13805, 13804, 13803, 13802, 13801, 13800, 13799, 13456, 13341, 12138, 11770, 11655, 11631, 10742, 10540, 10500, 10499, 10498, 10496, 10495, 10494, 10493, 10492, 10491, 10490, 10489, 10488, 10487, 10486, 10485, 10484, 10479, 9979, 9450, 9338, 8920, 8522, 8183, 7387, 6709, 6705, 4894, 4152, 4131, 3455, 2792, 2742, 2275, 462, 302, 187, 156, 69, 21, 20, 19, 11) and (order_code != '' and order_status = 'booked')) GROUP BY 1 LIMIT 10001;")
-- result:
mv_region_geohash,mv_fact_orders_1
-- !result
INSERT INTO dim_region VALUES 
(4, 'Mountain View', 'Region', 4, 1, 103, 'Hill Town', 'Rural', 'REG004', 'Residential', 'CountryA', 'TerritoryY', 'RegionW', 'Hill Town', 'North', 'Highland', 'Scenic Area', ['geohash7', 'geohash8'], [4001, 4002]);
-- result:
-- !result
INSERT INTO fact_orders VALUES 
(101, 1, 'G1', 'Standard', 'Sedan', '4', 1001, 'ws8g', 1.2, 'ORD006', 15.50, 5.3, '2023-10-15 08:30:45', 1.1, 15.50, 20230615, 'S006', 'completed', '2023-10-15 08:00:00', '2023-10-15 00:00:00');
-- result:
-- !result
SELECT subregion_name, CAST(SUM(total_amount) AS DOUBLE) / CAST(SUM(distance_km) AS DOUBLE) AS avg_fare_per_km FROM fact_orders AS f LEFT JOIN mv_region_geohash AS r ON CAST(r.geohash AS varchar)= CAST(f.geohash AS varchar) WHERE DATE_TRUNC('day', local_time) >= DATE_TRUNC('hour', STR_TO_DATE('2025-08-25 00:00:00', '%Y-%m-%d %H:%i:%S')) AND DATE_TRUNC('day', local_time) < DATE_TRUNC('hour', STR_TO_DATE('2025-09-01 00:00:00', '%Y-%m-%d %H:%i:%S')) AND date_id >= 20230623 AND date_id < 20250903 AND (country_id = 4 and city_id in (6) and vehicle_id in (16866, 16860, 16859, 16810, 15829, 15828, 15827, 15826, 15825, 15824, 15817, 15816, 15568, 15272, 15271, 15270, 15269, 15268, 15267, 15266, 15265, 13912, 13904, 13806, 13805, 13804, 13803, 13802, 13801, 13800, 13799, 13456, 13341, 12138, 11770, 11655, 11631, 10742, 10540, 10500, 10499, 10498, 10496, 10495, 10494, 10493, 10492, 10491, 10490, 10489, 10488, 10487, 10486, 10485, 10484, 10479, 9979, 9450, 9338, 8920, 8522, 8183, 7387, 6709, 6705, 4894, 4152, 4131, 3455, 2792, 2742, 2275, 462, 302, 187, 156, 69, 21, 20, 19, 11) and (order_code != '' and order_status = 'booked')) GROUP BY 1 ORDER BY 1 LIMIT 3;
-- result:
None	3.6956521739130435
Central Subregion	5.0
North Subregion	2.8018867924528297
-- !result
function: print_hit_materialized_views("SELECT subregion_name, CAST(SUM(total_amount) AS DOUBLE) / CAST(SUM(distance_km) AS DOUBLE) AS avg_fare_per_km FROM fact_orders AS f LEFT JOIN mv_region_geohash AS r ON CAST(r.geohash AS varchar)= CAST(f.geohash AS varchar) WHERE DATE_TRUNC('day', local_time) >= DATE_TRUNC('hour', STR_TO_DATE('2025-08-25 00:00:00', '%Y-%m-%d %H:%i:%S')) AND DATE_TRUNC('day', local_time) < DATE_TRUNC('hour', STR_TO_DATE('2025-09-01 00:00:00', '%Y-%m-%d %H:%i:%S')) AND date_id >= 20230623 AND date_id < 20250903 AND (country_id = 4 and city_id in (6) and vehicle_id in (16866, 16860, 16859, 16810, 15829, 15828, 15827, 15826, 15825, 15824, 15817, 15816, 15568, 15272, 15271, 15270, 15269, 15268, 15267, 15266, 15265, 13912, 13904, 13806, 13805, 13804, 13803, 13802, 13801, 13800, 13799, 13456, 13341, 12138, 11770, 11655, 11631, 10742, 10540, 10500, 10499, 10498, 10496, 10495, 10494, 10493, 10492, 10491, 10490, 10489, 10488, 10487, 10486, 10485, 10484, 10479, 9979, 9450, 9338, 8920, 8522, 8183, 7387, 6709, 6705, 4894, 4152, 4131, 3455, 2792, 2742, 2275, 462, 302, 187, 156, 69, 21, 20, 19, 11) and (order_code != '' and order_status = 'booked')) GROUP BY 1 LIMIT 10001;")
-- result:
mv_region_geohash,mv_fact_orders_1
-- !result
REFRESH MATERIALIZED VIEW mv_fact_orders_1 WITH SYNC MODE;
REFRESH MATERIALIZED VIEW mv_fact_orders_2 WITH SYNC MODE;
REFRESH MATERIALIZED VIEW mv_region_geohash WITH SYNC MODE;
SELECT subregion_name, CAST(SUM(total_amount) AS DOUBLE) / CAST(SUM(distance_km) AS DOUBLE) AS avg_fare_per_km FROM fact_orders AS f LEFT JOIN mv_region_geohash AS r ON CAST(r.geohash AS varchar)= CAST(f.geohash AS varchar) WHERE DATE_TRUNC('day', local_time) >= DATE_TRUNC('hour', STR_TO_DATE('2025-08-25 00:00:00', '%Y-%m-%d %H:%i:%S')) AND DATE_TRUNC('day', local_time) < DATE_TRUNC('hour', STR_TO_DATE('2025-09-01 00:00:00', '%Y-%m-%d %H:%i:%S')) AND date_id >= 20230623 AND date_id < 20250903 AND (country_id = 4 and city_id in (6) and vehicle_id in (16866, 16860, 16859, 16810, 15829, 15828, 15827, 15826, 15825, 15824, 15817, 15816, 15568, 15272, 15271, 15270, 15269, 15268, 15267, 15266, 15265, 13912, 13904, 13806, 13805, 13804, 13803, 13802, 13801, 13800, 13799, 13456, 13341, 12138, 11770, 11655, 11631, 10742, 10540, 10500, 10499, 10498, 10496, 10495, 10494, 10493, 10492, 10491, 10490, 10489, 10488, 10487, 10486, 10485, 10484, 10479, 9979, 9450, 9338, 8920, 8522, 8183, 7387, 6709, 6705, 4894, 4152, 4131, 3455, 2792, 2742, 2275, 462, 302, 187, 156, 69, 21, 20, 19, 11) and (order_code != '' and order_status = 'booked')) GROUP BY 1 ORDER BY 1 LIMIT 3;
-- result:
None	3.6956521739130435
Central Subregion	5.0
North Subregion	2.8018867924528297
-- !result
function: print_hit_materialized_views("SELECT subregion_name, CAST(SUM(total_amount) AS DOUBLE) / CAST(SUM(distance_km) AS DOUBLE) AS avg_fare_per_km FROM fact_orders AS f LEFT JOIN mv_region_geohash AS r ON CAST(r.geohash AS varchar)= CAST(f.geohash AS varchar) WHERE DATE_TRUNC('day', local_time) >= DATE_TRUNC('hour', STR_TO_DATE('2025-08-25 00:00:00', '%Y-%m-%d %H:%i:%S')) AND DATE_TRUNC('day', local_time) < DATE_TRUNC('hour', STR_TO_DATE('2025-09-01 00:00:00', '%Y-%m-%d %H:%i:%S')) AND date_id >= 20230623 AND date_id < 20250903 AND (country_id = 4 and city_id in (6) and vehicle_id in (16866, 16860, 16859, 16810, 15829, 15828, 15827, 15826, 15825, 15824, 15817, 15816, 15568, 15272, 15271, 15270, 15269, 15268, 15267, 15266, 15265, 13912, 13904, 13806, 13805, 13804, 13803, 13802, 13801, 13800, 13799, 13456, 13341, 12138, 11770, 11655, 11631, 10742, 10540, 10500, 10499, 10498, 10496, 10495, 10494, 10493, 10492, 10491, 10490, 10489, 10488, 10487, 10486, 10485, 10484, 10479, 9979, 9450, 9338, 8920, 8522, 8183, 7387, 6709, 6705, 4894, 4152, 4131, 3455, 2792, 2742, 2275, 462, 302, 187, 156, 69, 21, 20, 19, 11) and (order_code != '' and order_status = 'booked')) GROUP BY 1 LIMIT 10001;")
-- result:
mv_fact_orders_1,mv_region_geohash
-- !result
drop database db_${uuid0};
-- result:
-- !result