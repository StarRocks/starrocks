-- inventory
create table inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       
)
duplicate key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
distributed by hash(inv_date_sk, inv_item_sk, inv_warehouse_sk) buckets 5
properties(
    "replication_num" = "1"
);
