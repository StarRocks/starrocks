// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/push_handler.h"

#include "storage/compaction_manager.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/schema_change.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "util/defer_op.h"

namespace starrocks {

// Process push command, the main logical is as follows:
//    a. related tablets not exist:
//        current table isn't in schema change state, only push for current
//        tablet
//    b. related tablets exist
//       I.  current tablet is old table (cur.creation_time < related.creation_time):
//           push for current table and than convert data for related tables
//       II. current table is new table:
//           this usually means schema change is over,
//           clear schema change info in both current tablet and related
//           tablets, finally we will only push for current tablets. this is
//           very useful in rollup action.
Status PushHandler::process_streaming_ingestion(const TabletSharedPtr& tablet, const TPushReq& request,
                                                PushType push_type, std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to realtime vectorized push. tablet=" << tablet->full_name()
              << ", txn_id: " << request.transaction_id;

    _request = request;
    std::vector<TabletVars> tablet_vars(1);
    tablet_vars[0].tablet = tablet;
    Status st = _do_streaming_ingestion(tablet, request, push_type, &tablet_vars, tablet_info_vec);

    if (st.ok()) {
        if (tablet_info_vec != nullptr) {
            _get_tablet_infos(tablet_vars, tablet_info_vec);
        }
        LOG(INFO) << "process realtime vectorized push successfully. "
                  << "tablet=" << tablet->full_name() << ", partition_id=" << request.partition_id
                  << ", txn_id: " << request.transaction_id;
    }

    return st;
}

Status PushHandler::_do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request, PushType push_type,
                                            std::vector<TabletVars>* tablet_vars,
                                            std::vector<TTabletInfo>* tablet_info_vec) {
    // add transaction in engine, then check sc status
    // lock, prevent sc handler checking transaction concurrently
    if (tablet == nullptr) {
        return Status::InternalError("Table not found");
    }
    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::InternalError("Rwlock error");
    }
    if (Tablet::check_migrate(tablet)) {
        return Status::InternalError("tablet is migrating or has been migrated");
    }

    Status res = Status::OK();
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    tablet->obtain_push_lock();
    {
        DeferOp release_push_lock([&tablet] { return tablet->release_push_lock(); });
        RETURN_IF_ERROR(StorageEngine::instance()->txn_manager()->prepare_txn(request.partition_id, tablet,
                                                                              request.transaction_id, load_id));
    }

    if (tablet_vars->size() == 1) {
        tablet_vars->resize(2);
    }

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
            if (!res.ok()) {
                LOG(WARNING) << "Fail to generate delete condition. res=" << res
                             << ", tablet=" << tablet_var.tablet->full_name();
                return Status::InternalError("Fail to generate delete condition");
            }
        }
    }

    Status st = Status::OK();
    if (push_type == PUSH_NORMAL_V2) {
        st = _load_convert(tablet_vars->at(0).tablet, &(tablet_vars->at(0).rowset_to_add));
    } else {
        DCHECK_EQ(push_type, PUSH_FOR_DELETE);
        st = _delete_convert(tablet_vars->at(0).tablet, &(tablet_vars->at(0).rowset_to_add));
    }

    if (!st.ok()) {
        LOG(WARNING) << "fail to convert tmp file when realtime push. res=" << st.to_string()
                     << ", failed to process realtime push."
                     << ", table=" << tablet->full_name() << ", txn_id: " << request.transaction_id;
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            Status rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(
                    request.partition_id, tablet_var.tablet, request.transaction_id);
            // has to check rollback status to ensure not delete a committed rowset
            if (rollback_status.ok()) {
                // actually, olap_index may has been deleted in delete_transaction()
                StorageEngine::instance()->add_unused_rowset(tablet_var.rowset_to_add);
            }
        }
        return st;
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
        Status commit_status = StorageEngine::instance()->txn_manager()->commit_txn(
                request.partition_id, tablet_var.tablet, request.transaction_id, load_id, tablet_var.rowset_to_add,
                false);
        if (!commit_status.ok() && !commit_status.is_already_exist()) {
            LOG(WARNING) << "fail to commit txn. res=" << commit_status << ", table=" << tablet->full_name()
                         << ", txn_id: " << request.transaction_id;
            st = Status::InternalError("Fail to commit txn");
        }
        StorageEngine::instance()->compaction_manager()->update_tablet_async(tablet_var.tablet);
    }
    return st;
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

Status PushHandler::_delete_convert(const TabletSharedPtr& cur_tablet, RowsetSharedPtr* cur_rowset) {
    Status st = Status::OK();
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        // 1. Init BinaryReader to read raw file if exist,
        //    in case of empty push and delete data, this will be skipped.
        DCHECK(!_request.__isset.http_file_path);

        // 2. init RowsetBuilder of cur_tablet for current push
        RowsetWriterContext context;
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_uid = cur_tablet->tablet_uid();
        context.tablet_id = cur_tablet->tablet_id();
        context.partition_id = _request.partition_id;
        context.tablet_schema_hash = cur_tablet->schema_hash();
        context.rowset_path_prefix = cur_tablet->schema_hash_path();
        context.tablet_schema = &cur_tablet->tablet_schema();
        context.rowset_state = PREPARED;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        // although the hadoop load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        st = RowsetFactory::create_rowset_writer(context, &rowset_writer);
        if (!st.ok()) {
            LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                         << ", txn_id: " << _request.transaction_id << ", status:" << st.to_string();
            break;
        }

        // 3. New RowsetBuilder to write data into rowset
        VLOG(3) << "init rowset builder. tablet=" << cur_tablet->full_name()
                << ", block_row_size=" << cur_tablet->num_rows_per_row_block();
        st = rowset_writer->flush();
        if (!st.ok()) {
            LOG(WARNING) << "Failed to finalize writer: " << st;
            break;
        }
        auto rowset = rowset_writer->build();
        if (!rowset.ok()) return rowset.status();
        *cur_rowset = std::move(rowset).value();

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();

    } while (false);

    return st;
}

Status PushHandler::_load_convert(const TabletSharedPtr& cur_tablet, RowsetSharedPtr* cur_rowset) {
    Status st;
    size_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    VLOG(3) << "start to convert delta file.";

    // 1. init RowsetBuilder of cur_tablet for current push
    VLOG(3) << "init rowset builder. tablet=" << cur_tablet->full_name()
            << ", block_row_size=" << cur_tablet->num_rows_per_row_block();
    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = cur_tablet->tablet_uid();
    context.tablet_id = cur_tablet->tablet_id();
    context.partition_id = _request.partition_id;
    context.tablet_schema_hash = cur_tablet->schema_hash();
    context.rowset_path_prefix = cur_tablet->schema_hash_path();
    context.tablet_schema = &(cur_tablet->tablet_schema());
    context.rowset_state = PREPARED;
    context.txn_id = _request.transaction_id;
    context.load_id = load_id;
    context.segments_overlap = NONOVERLAPPING;

    std::unique_ptr<RowsetWriter> rowset_writer;
    st = RowsetFactory::create_rowset_writer(context, &rowset_writer);
    if (!st.ok()) {
        LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                     << ", txn_id: " << _request.transaction_id << ", res=" << st;
        return Status::InternalError("Fail to init rowset writer");
    }

    // 2. Init PushBrokerReader to read broker file if exist,
    //    in case of empty push this will be skipped.
    std::string path = _request.broker_scan_range.ranges[0].path;
    LOG(INFO) << "tablet=" << cur_tablet->full_name() << ", file path=" << path
              << ", file size=" << _request.broker_scan_range.ranges[0].file_size;

    if (!path.empty()) {
        auto reader = std::make_unique<PushBrokerReader>();
        DeferOp reader_close([&reader] { return reader->close(); });

        // 3. read data and write rowset
        // init Reader
        // star_offset and size are not set in FE plan before,
        // here we set if start_offset or size <= 0 for smooth upgrade.
        TBrokerScanRange t_scan_range = _request.broker_scan_range;
        DCHECK_EQ(1, t_scan_range.ranges.size());
        if (t_scan_range.ranges[0].start_offset <= 0 || t_scan_range.ranges[0].size <= 0) {
            t_scan_range.ranges[0].__set_start_offset(0);
            t_scan_range.ranges[0].__set_size(_request.broker_scan_range.ranges[0].file_size);
        }
        st = reader->init(t_scan_range, _request);
        if (!st.ok()) {
            LOG(WARNING) << "fail to init reader. res=" << st.to_string() << ", tablet=" << cur_tablet->full_name();
            return st;
        }

        // read data from broker and write into Rowset of cur_tablet
        VLOG(3) << "start to convert etl file to delta.";
        auto schema = ChunkHelper::convert_schema(cur_tablet->tablet_schema());
        ChunkPtr chunk = ChunkHelper::new_chunk(schema, 0);
        while (!reader->eof()) {
            st = reader->next_chunk(&chunk);
            if (!st.ok()) {
                LOG(WARNING) << "fail to read next chunk. err=" << st.to_string() << ", read_rows=" << num_rows;
                return st;
            } else {
                if (reader->eof()) {
                    break;
                }

                st = rowset_writer->add_chunk(*chunk);
                if (!st.ok()) {
                    LOG(WARNING) << "fail to add chunk to rowset writer"
                                 << ". res=" << st << ", tablet=" << cur_tablet->full_name()
                                 << ", read_rows=" << num_rows;
                    return st;
                }

                num_rows += chunk->num_rows();
                chunk->reset();
            }
        }

        reader->print_profile();
    }

    // 4. finish
    RETURN_IF_ERROR(rowset_writer->flush());
    auto rowset = rowset_writer->build();
    if (!rowset.ok()) return rowset.status();
    *cur_rowset = std::move(rowset).value();

    _write_bytes += static_cast<int64_t>((*cur_rowset)->data_disk_size());
    _write_rows += static_cast<int64_t>((*cur_rowset)->num_rows());
    VLOG(10) << "convert delta file end. res=" << st.to_string() << ", tablet=" << cur_tablet->full_name()
             << ", processed_rows" << num_rows;
    return st;
}
} // namespace starrocks
