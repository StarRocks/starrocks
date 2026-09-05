-- reason
create table reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     
)
duplicate key (r_reason_sk)
distributed by hash(r_reason_sk) buckets 5
properties(
    "replication_num" = "1"
);
