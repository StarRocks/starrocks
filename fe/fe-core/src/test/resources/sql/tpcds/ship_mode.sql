-- ship_mode
create table ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
)
duplicate key (sm_ship_mode_sk)
distributed by hash(sm_ship_mode_sk) buckets 5
properties(
    "replication_num" = "1"
);
