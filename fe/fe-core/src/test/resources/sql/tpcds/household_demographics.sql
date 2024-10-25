-- household_demographics
create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
)
duplicate key (hd_demo_sk)
distributed by hash(hd_demo_sk) buckets 5
properties(
    "replication_num" = "1"
);
