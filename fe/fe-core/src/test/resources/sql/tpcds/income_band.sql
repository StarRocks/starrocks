-- income_band
create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
)
duplicate key (ib_income_band_sk)
distributed by hash(ib_income_band_sk) buckets 5
properties(
    "replication_num" = "1"
);
