---
displayed_sidebar: docs
---

# HTTP API

In order to facilitate the maintenance of StarRocks clusters, StarRocks provides many operation and query interfaces. This document lists the HTTP interfaces and how to use them.

## FE

| HTTP Method | HTTP PATH                                     | Description                                                                                                                         |
|-------------|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| PUT         | `/api/{db}/{table}/_stream_load`             | Stream Load Action, please refer to [Stream Load](../loading/StreamLoad.md)。                                                        |
| POST/PUT    | `/api/transaction/{txn_op}`                  | Stream Load Transaction Action，please refer to [Stream Load transaction interface](../loading/Stream_Load_transaction_interface.md) |
| GET         | `/api/{db}/_load_info`                       
| GET         | `/api/_set_config?config_key1=config_value1` | Update FE Configuration, please refer to [FE Parameter Configuration](../administration/management/FE_configuration.md)             |
| GET         | `/api/_get_ddl?db={}&tbl={}`                 | Show DDL of the table.                                                                                                              
| GET         | `/api/_migration?db={}&tbl={}`               | Show tablets of the table.                                                                                                          |
| GET         | `/api/_check_storagetype`                    
| POST        | `/api/{db}/{table}/_cancel?label={}`         
| GET         | `/api/{db}/get_load_state`                   
| GET         | `/api/health`                                
| GET         | `/metrics?type={core/json}`                  | Show metrics of the FE.                                                                                                             |
| GET         | `/api/show_meta_info`                        
| GET         | `/api/show_proc`                             
| GET         | `/api/show_runtime_info`                     
| HEAD/GET    | `/api/get_log_file`                          
| GET         | `/api/get_small_file`                        
| GET         | `/api/rowcount`                              
| GET         | `/api/check_decommission`                    
| GET         | `/api/_meta_replay_state`                    
| POST        | `/api/colocate/bucketseq`                    
| GET         | `/api/colocate`                              
| POST        | `/api/colocate/group_stable`                 
| POST        | `/api/colocate/group_unstable`               
| POST        | `/api/colocate/update_group`                 
| POST        | `/api/global_dict/table/enable`              
| GET         | `/api/profile?query_id={}`                   | Get profile by query ID.                                                                                                            |
| GET         | `/api/query_detail`                          
| GET         | `/api/connection`                            
| GET         | `/api/show_data?db={}`                       | Show data size of tables in the db.                                                                                                 |
| POST        | `/api/query_dump`                            | Get query dump, please refer to [Query dump](../faq/Dump_query.md) .                                                                |
| GET         | `/api/stop`                                  
| GET         | `/image`                                     
| GET         | `/info`                                      
| GET         | `/version`                                   
| GET         | `/put`                                       
| GET         | `/journal_id`                                
| GET         | `/check`                                     
| GET         | `/dump`                                      
| GET         | `/role`                                      
| GET         | `/api/{db}/{table}/_count`                   
| GET         | `/api/{db}/{table}/_schema`                  | get schema of the table.                                                                                                            |
| GET/POST    | `/api/{db}/{table}/_query_plan`              

## BE

| HTTP Method   | HTTP PATH                                                     | Description                                                                                                                          |
|---------------|---------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| PUT           | `/api/{db}/{table}/_stream_load`                              | Stream Load Action, please refer to [Stream Load](../loading/StreamLoad.md)                                                          |
| POST/PUT      | `/api/transaction/{txn_op}`                                   | Stream Load Transaction Action, please refer to [Stream Load transaction interface](../loading/Stream_Load_transaction_interface.md) |
| PUT           | `/api/transaction/load`                                       |
| HEAD/GET      | `/api/_download_load`                                         |
| HEAD/GET      | `/api/_tablet/_download`                                      |
| HEAD/GET      | `/api/_load_error_log`                                        |
| GET           | `/api/health`                                                 |
| GET           | `/api/_stop_be`                                               |
| GET           | `/pprof/heap`                                                 |
| GET           | `/pprof/growth`                                               |
| GET           | `/pprof/profile`                                              |
| GET           | `/pprof/pmuprofile`                                           |
| GET           | `/pprof/contention`                                           |
| GET           | `/pprof/cmdline`                                              |
| HEAD/GET/POST | `/pprof/symbol`                                               |
| GET           | `/metrics`                                                    | Show metrics of the BE.                                                                                                              |
| HEAD          | `/api/meta/header/{tablet_id}`                                |
| GET           | `/api/checksum`                                               |
| GET           | `/api/reload_tablet`                                          |
| POST          | `/api/restore_tablet`                                         |
| GET           | `/api/snapshot`                                               |
| GET           | `/api/compaction/show?tablet_id={}`                           | Show compaction stat of the tablet.                                                                                                  
| POST          | `/api/compact?tablet_id={}&compaction_type={base/cumulative}` | Trigger compaction for the tablet.                                                                                                   |
| GET           | `/api/compaction/show_repair`                                 |
| PUT           | `/api/compaction/submit_repair`                               |
| POST          | `/api/update_config`                                          | Update BE Configuration, please refer to [BE Parameter Configuration](../administration/management/BE_configuration.md).             |
| GET/PUT       | `/api/runtime_filter_cache/{action}`                          |
| POST          | `/api/compact_rocksdb_meta`                                   |
| GET/PUT       | `/api/query_cache/{action}`                                   |
| GET           | `/api/pipeline_blocking_drivers/{action}`                     |
| GET           | `/greplog`                                                    |
| GET           | `/varz`                                                       | Show configurations of the BE.                                                                                                       |

## CN

| HTTP Method   | HTTP PATH            | Description             |
|---------------|---------------------|-------------------------|
| GET           | `/api/health`       |
| GET           | `/pprof/heap`       |
| GET           | `/pprof/growth`     |
| GET           | `/pprof/profile`    |
| GET           | `/pprof/pmuprofile` |
| GET           | `/pprof/contention` |
| GET           | `/pprof/cmdline`    |
| HEAD/GET/POST | `/pprof/symbol`     |
| GET           | `/metrics`          | Show metrics of the CN. |
