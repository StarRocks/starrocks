// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/push_handler.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/push_handler.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>

#include "common/status.h"
#include "runtime/exec_env.h"
#include "storage/row.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/schema_change.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"

using std::list;
using std::map;
using std::string;
using std::vector;

namespace starrocks {

// Process push command, the main logical is as follows:
//    a. related tablets not exist:
//        current table isn't in schemachange state, only push for current
//        tablet
//    b. related tablets exist
//       I.  current tablet is old table (cur.creation_time <
//       related.creation_time):
//           push for current table and than convert data for related tables
//       II. current table is new table:
//           this usually means schema change is over,
//           clear schema change info in both current tablet and related
//           tablets, finally we will only push for current tablets. this is
//           very useful in rollup action.
OLAPStatus PushHandler::process_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request, PushType push_type,
                                                    std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to realtime push. tablet=" << tablet->full_name()
              << ", transaction_id=" << request.transaction_id;

    OLAPStatus res = OLAP_SUCCESS;
    _request = request;
    std::vector<TabletVars> tablet_vars(1);
    tablet_vars[0].tablet = tablet;
    res = _do_streaming_ingestion(tablet, request, push_type, &tablet_vars, tablet_info_vec);

    if (res == OLAP_SUCCESS) {
        if (tablet_info_vec != nullptr) {
            _get_tablet_infos(tablet_vars, tablet_info_vec);
        }
        LOG(INFO) << "process realtime push successfully. "
                  << "tablet=" << tablet->full_name() << ", partition_id=" << request.partition_id
                  << ", transaction_id=" << request.transaction_id;
    }

    return res;
}

OLAPStatus PushHandler::_do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request, PushType push_type,
                                                std::vector<TabletVars>* tablet_vars,
                                                std::vector<TTabletInfo>* tablet_info_vec) {
    // add transaction in engine, then check sc status
    // lock, prevent sc handler checking transaction concurrently
    if (tablet == nullptr) {
        return OLAP_ERR_TABLE_NOT_FOUND;
    }
    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return OLAP_ERR_RWLOCK_ERROR;
    }
    if (Tablet::check_migrate(tablet)) {
        return OLAP_ERR_OTHER_ERROR;
    }

    tablet->obtain_push_lock();
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    OLAPStatus res = StorageEngine::instance()->txn_manager()->prepare_txn(request.partition_id, tablet,
                                                                           request.transaction_id, load_id);
    if (res != OLAP_SUCCESS) {
        tablet->release_push_lock();
        return res;
    }

    // prepare txn will be always successful
    // if current tablet is under schema change, origin tablet is successful and
    // new tablet is not successful, it may be a fatal error because new tablet has
    // not load successfully

    // only when fe sends schema_change true, should consider to push related
    // tablet
    if (_request.is_schema_changing) {
        VLOG(3) << "push req specify schema changing is true. "
                << "tablet=" << tablet->full_name() << ", transaction_id=" << request.transaction_id;
        AlterTabletTaskSharedPtr alter_task = tablet->alter_task();
        if (alter_task != nullptr && alter_task->alter_state() != ALTER_FAILED) {
            TTabletId related_tablet_id = alter_task->related_tablet_id();
            TSchemaHash related_schema_hash = alter_task->related_schema_hash();
            LOG(INFO) << "find schema_change status when realtime push. "
                      << "tablet=" << tablet->full_name() << ", related_tablet_id=" << related_tablet_id
                      << ", related_schema_hash=" << related_schema_hash
                      << ", transaction_id=" << request.transaction_id;
            TabletSharedPtr related_tablet =
                    StorageEngine::instance()->tablet_manager()->get_tablet(related_tablet_id, related_schema_hash);

            // if related tablet not exists, only push current tablet
            if (related_tablet == nullptr) {
                LOG(WARNING) << "find alter task but not find related tablet, "
                             << "related_tablet_id=" << related_tablet_id
                             << ", related_schema_hash=" << related_schema_hash;
                tablet->release_push_lock();
                return OLAP_ERR_TABLE_NOT_FOUND;
                // if current tablet is new tablet, only push current tablet
            } else if (tablet->creation_time() > related_tablet->creation_time()) {
                LOG(INFO) << "current tablet is new, only push current tablet. "
                          << "tablet=" << tablet->full_name() << " related_tablet=" << related_tablet->full_name();
            } else {
                std::shared_lock new_migration_rlock(related_tablet->get_migration_lock(), std::try_to_lock);
                if (!new_migration_rlock.owns_lock()) {
                    tablet->release_push_lock();
                    return OLAP_ERR_RWLOCK_ERROR;
                }
                if (Tablet::check_migrate(related_tablet)) {
                    tablet->release_push_lock();
                    return OLAP_ERR_OTHER_ERROR;
                }

                PUniqueId load_id;
                load_id.set_hi(0);
                load_id.set_lo(0);
                res = StorageEngine::instance()->txn_manager()->prepare_txn(request.partition_id, related_tablet,
                                                                            request.transaction_id, load_id);
                if (res != OLAP_SUCCESS) {
                    tablet->release_push_lock();
                    return res;
                }
                // prepare txn will always be successful
                tablet_vars->push_back(TabletVars());
                TabletVars& new_item = tablet_vars->back();
                new_item.tablet = related_tablet;
            }
        }
    }
    tablet->release_push_lock();

    if (tablet_vars->size() == 1) {
        tablet_vars->resize(2);
    }

    // not call validate request here, because realtime load does not
    // contain version info

    // check delete condition if push for delete
    std::queue<DeletePredicatePB> del_preds;
    if (push_type == PUSH_FOR_DELETE) {
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            DeletePredicatePB del_pred;
            DeleteConditionHandler del_cond_handler;
            tablet_var.tablet->obtain_header_rdlock();
            res = del_cond_handler.generate_delete_predicate(tablet_var.tablet->tablet_schema(),
                                                             request.delete_conditions, &del_pred);
            del_preds.push(del_pred);
            tablet_var.tablet->release_header_lock();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to generate delete condition. res=" << res
                             << ", tablet=" << tablet_var.tablet->full_name();
                return res;
            }
        }
    }

    if (push_type == PUSH_NORMAL_V2) {
        res = _convert_v2(tablet_vars->at(0).tablet, &tablet_vars->at(0).rowset_to_add);
    } else {
        DCHECK_EQ(push_type, PUSH_FOR_DELETE);
        res = _convert(tablet_vars->at(0).tablet, &tablet_vars->at(0).rowset_to_add);
    }
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to convert tmp file when realtime push. res=" << res
                     << ", failed to process realtime push."
                     << ", table=" << tablet->full_name() << ", transaction_id=" << request.transaction_id;
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            OLAPStatus rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(
                    request.partition_id, tablet_var.tablet, request.transaction_id);
            // has to check rollback status to ensure not delete a committed rowset
            if (rollback_status == OLAP_SUCCESS) {
                // actually, olap_index may has been deleted in delete_transaction()
                StorageEngine::instance()->add_unused_rowset(tablet_var.rowset_to_add);
            }
        }
        return res;
    }

    // add pending data to tablet
    for (TabletVars& tablet_var : *tablet_vars) {
        if (tablet_var.tablet == nullptr) {
            continue;
        }

        if (push_type == PUSH_FOR_DELETE) {
            tablet_var.rowset_to_add->rowset_meta()->set_delete_predicate(del_preds.front());
            del_preds.pop();
        }
        OLAPStatus commit_status = StorageEngine::instance()->txn_manager()->commit_txn(
                request.partition_id, tablet_var.tablet, request.transaction_id, load_id, tablet_var.rowset_to_add,
                false);
        if (commit_status != OLAP_SUCCESS && commit_status != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            res = commit_status;
        }
    }
    return res;
}

OLAPStatus PushHandler::_convert(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset) {
    OLAPStatus res = OLAP_SUCCESS;
    uint32_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG(3) << "start to convert delta file.";

        // 1. Init BinaryReader to read raw file if exist,
        //    in case of empty push and delete data, this will be skipped.
        DCHECK(!_request.__isset.http_file_path);

        // 2. init RowsetBuilder of cur_tablet for current push
        VLOG(3) << "init RowsetBuilder.";
        RowsetWriterContext context(kDataFormatUnknown, config::storage_format_version);
        context.mem_tracker = ExecEnv::GetInstance()->load_mem_tracker();
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_uid = cur_tablet->tablet_uid();
        context.tablet_id = cur_tablet->tablet_id();
        context.partition_id = _request.partition_id;
        context.tablet_schema_hash = cur_tablet->schema_hash();
        context.rowset_type = BETA_ROWSET;
        context.rowset_path_prefix = cur_tablet->tablet_path();
        context.tablet_schema = &cur_tablet->tablet_schema();
        context.rowset_state = PREPARED;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        // although the hadoop load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        res = RowsetFactory::create_rowset_writer(context, &rowset_writer);
        if (OLAP_SUCCESS != res) {
            LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                         << ", txn_id=" << _request.transaction_id << ", res=" << res;
            break;
        }

        // 3. New RowsetBuilder to write data into rowset
        VLOG(3) << "init rowset builder. tablet=" << cur_tablet->full_name()
                << ", block_row_size=" << cur_tablet->num_rows_per_row_block();
        if (rowset_writer->flush() != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to finalize writer.";
            break;
        }
        *cur_rowset = rowset_writer->build();

        if (*cur_rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();

    } while (0);

    VLOG(10) << "convert delta file end. res=" << res << ", tablet=" << cur_tablet->full_name() << ", processed_rows"
             << num_rows;
    return res;
}

OLAPStatus PushHandler::_convert_v2(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset) {
    OLAPStatus res = OLAP_SUCCESS;
    uint32_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG(3) << "start to convert delta file.";

        // 1. init RowsetBuilder of cur_tablet for current push
        VLOG(3) << "init rowset builder. tablet=" << cur_tablet->full_name()
                << ", block_row_size=" << cur_tablet->num_rows_per_row_block();
        RowsetWriterContext context(kDataFormatUnknown, config::storage_format_version);
        context.mem_tracker = ExecEnv::GetInstance()->load_mem_tracker();
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_uid = cur_tablet->tablet_uid();
        context.tablet_id = cur_tablet->tablet_id();
        context.partition_id = _request.partition_id;
        context.tablet_schema_hash = cur_tablet->schema_hash();
        context.rowset_type = BETA_ROWSET;
        context.rowset_path_prefix = cur_tablet->tablet_path();
        context.tablet_schema = &cur_tablet->tablet_schema();
        context.rowset_state = PREPARED;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        // although the spark load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        res = RowsetFactory::create_rowset_writer(context, &rowset_writer);
        if (OLAP_SUCCESS != res) {
            LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                         << ", txn_id=" << _request.transaction_id << ", res=" << res;
            break;
        }

        // 2. Init PushBrokerReader to read broker file if exist,
        //    in case of empty push this will be skipped.
        std::string path = _request.broker_scan_range.ranges[0].path;
        LOG(INFO) << "tablet=" << cur_tablet->full_name() << ", file path=" << path
                  << ", file size=" << _request.broker_scan_range.ranges[0].file_size;

        if (!path.empty()) {
            std::unique_ptr<PushBrokerReader> reader(new (std::nothrow) PushBrokerReader());
            if (reader == nullptr) {
                LOG(WARNING) << "fail to create reader. tablet=" << cur_tablet->full_name();
                res = OLAP_ERR_MALLOC_ERROR;
                break;
            }

            // init schema
            std::unique_ptr<Schema> schema(new (std::nothrow) Schema(cur_tablet->tablet_schema()));
            if (schema == nullptr) {
                LOG(WARNING) << "fail to create schema. tablet=" << cur_tablet->full_name();
                res = OLAP_ERR_MALLOC_ERROR;
                break;
            }

            // init Reader
            if (OLAP_SUCCESS != (res = reader->init(schema.get(), _request.broker_scan_range, _request.desc_tbl))) {
                LOG(WARNING) << "fail to init reader. res=" << res << ", tablet=" << cur_tablet->full_name();
                res = OLAP_ERR_PUSH_INIT_ERROR;
                break;
            }

            // 3. Init Row
            uint8_t* tuple_buf = reader->mem_pool()->allocate(schema->schema_size());
            if (UNLIKELY(tuple_buf == nullptr)) {
                res = OLAP_ERR_MALLOC_ERROR;
                break;
            }
            ContiguousRow row(schema.get(), tuple_buf);

            // 4. Read data from broker and write into Rowset of cur_tablet
            VLOG(3) << "start to convert etl file to delta.";
            while (!reader->eof()) {
                res = reader->next(&row);
                if (OLAP_SUCCESS != res) {
                    LOG(WARNING) << "read next row failed."
                                 << " res=" << res << " read_rows=" << num_rows;
                    break;
                } else {
                    if (reader->eof()) {
                        break;
                    }
                    if (OLAP_SUCCESS != (res = rowset_writer->add_row(row))) {
                        LOG(WARNING) << "fail to attach row to rowset_writer. "
                                     << "res=" << res << ", tablet=" << cur_tablet->full_name()
                                     << ", read_rows=" << num_rows;
                        break;
                    }
                    num_rows++;
                }
            }
            if (res != OLAP_SUCCESS) {
                reader->close();
                break;
            }

            reader->print_profile();
            reader->close();
        }

        if (rowset_writer->flush() != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to finalize writer";
            break;
        }
        *cur_rowset = rowset_writer->build();
        if (*cur_rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();

    } while (0);

    VLOG(10) << "convert delta file end. res=" << res << ", tablet=" << cur_tablet->full_name() << ", processed_rows"
             << num_rows;
    return res;
}

void PushHandler::_get_tablet_infos(const std::vector<TabletVars>& tablet_vars,
                                    std::vector<TTabletInfo>* tablet_info_vec) {
    for (const TabletVars& tablet_var : tablet_vars) {
        if (tablet_var.tablet.get() == nullptr) {
            continue;
        }
        TTabletInfo tablet_info;
        tablet_info.tablet_id = tablet_var.tablet->tablet_id();
        tablet_info.schema_hash = tablet_var.tablet->schema_hash();
        (void)StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info);
        tablet_info_vec->push_back(tablet_info);
    }
}

OLAPStatus PushBrokerReader::init(const Schema* schema, const TBrokerScanRange& t_scan_range,
                                  const TDescriptorTable& t_desc_tbl) {
    // init schema
    _schema = schema;

    // init runtime state, runtime profile, counter
    TUniqueId dummy_id;
    dummy_id.hi = 0;
    dummy_id.lo = 0;
    TPlanFragmentExecParams params;
    params.fragment_instance_id = dummy_id;
    params.query_id = dummy_id;
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = InternalServiceVersion::V1;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state =
            std::make_unique<RuntimeState>(fragment_params, query_options, query_globals, ExecEnv::GetInstance());
    DescriptorTbl* desc_tbl = nullptr;
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), t_desc_tbl, &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to create descriptor table, msg: " << status.get_error_msg();
        return OLAP_ERR_PUSH_INIT_ERROR;
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("PushBrokerReader");
    status = _runtime_state->init_mem_trackers(dummy_id);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
        return OLAP_ERR_PUSH_INIT_ERROR;
    }
    _mem_pool = std::make_unique<MemPool>(_runtime_state->instance_mem_tracker());

    switch (t_scan_range.ranges[0].format_type) {
    default:
        LOG(WARNING) << "Unsupported file format type: " << t_scan_range.ranges[0].format_type;
        return OLAP_ERR_PUSH_INIT_ERROR;
    }
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to open scanner, msg: " << status.get_error_msg();
        return OLAP_ERR_PUSH_INIT_ERROR;
    }

    // init tuple
    auto tuple_id = t_scan_range.params.dest_tuple_id;
    _tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        LOG(WARNING) << "Failed to get tuple descriptor, tuple_id: " << tuple_id;
        return OLAP_ERR_PUSH_INIT_ERROR;
    }

    int tuple_buffer_size = _tuple_desc->byte_size();
    void* tuple_buffer = _mem_pool->allocate(tuple_buffer_size);
    if (tuple_buffer == nullptr) {
        LOG(WARNING) << "Allocate memory for tuple failed";
        return OLAP_ERR_PUSH_INIT_ERROR;
    }
    _tuple = reinterpret_cast<Tuple*>(tuple_buffer);

    _ready = true;
    return OLAP_SUCCESS;
}

OLAPStatus PushBrokerReader::next(ContiguousRow* row) {
    if (!_ready || row == nullptr) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    memset(_tuple, 0, _tuple_desc->num_null_bytes());
    // Get from scanner
    Status status = Status::OK();
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "FileScanner get next tuple failed";
        return OLAP_ERR_PUSH_INPUT_DATA_ERROR;
    }
    if (_eof) {
        return OLAP_SUCCESS;
    }

    auto slot_descs = _tuple_desc->slots();
    size_t num_key_columns = _schema->num_key_columns();

    // finalize row
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = slot_descs[i];
        bool is_null = _tuple->is_null(slot->null_indicator_offset());
        const void* value = _tuple->get_slot(slot->tuple_offset());
        // try execute init method defined in aggregateInfo
        // by default it only copies data into cell
        _schema->column(i)->consume(&cell, (const char*)value, is_null, _mem_pool.get(), _runtime_state->obj_pool());
        // if column(i) is a value column, try execute finalize method defined in aggregateInfo
        // to convert data into final format
        if (i >= num_key_columns) {
            _schema->column(i)->agg_finalize(&cell, _mem_pool.get());
        }
    }

    return OLAP_SUCCESS;
}

void PushBrokerReader::print_profile() {
    std::stringstream ss;
    _runtime_profile->pretty_print(&ss);
    LOG(INFO) << ss.str();
}

} // namespace starrocks
