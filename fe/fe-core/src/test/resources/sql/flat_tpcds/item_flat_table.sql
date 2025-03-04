CREATE TABLE `item_flat` (
  `i_item_sk` bigint(20) DEFAULT NULL,
  `i_item_id` varchar(4096) DEFAULT NULL,
  `i_rec_end_date` date DEFAULT NULL,
  `i_item_desc` varchar(4096) DEFAULT NULL,
  `i_current_price` decimal(7, 2) DEFAULT NULL,
  `i_wholesale_cost` decimal(7, 2) DEFAULT NULL,
  `i_brand_id` int(11) DEFAULT NULL,
  `i_brand` varchar(4096) DEFAULT NULL,
  `i_class_id` int(11) DEFAULT NULL,
  `i_class` varchar(4096) DEFAULT NULL,
  `i_category_id` int(11) DEFAULT NULL,
  `i_category` varchar(4096) DEFAULT NULL,
  `i_manufact_id` int(11) DEFAULT NULL,
  `i_manufact` varchar(4096) DEFAULT NULL,
  `i_size` varchar(4096) DEFAULT NULL,
  `i_formulation` varchar(4096) DEFAULT NULL,
  `i_color` varchar(4096) DEFAULT NULL,
  `i_units` varchar(4096) DEFAULT NULL,
  `i_container` varchar(4096) DEFAULT NULL,
  `i_manager_id` int(11) DEFAULT NULL,
  `i_product_name` varchar(4096) DEFAULT NULL,
  `i_rec_start_date` date DEFAULT NULL
) PARTITION BY(i_rec_start_date)

PROPERTIES (
  "replication_num" = "1"
);
