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

#include "storage/lake/txn_log_applier.h"

#include <fmt/format.h>

#include <climits>

#include "gutil/strings/join.h"
#include "runtime/current_thread.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/lake_primary_key_recover.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "testutil/sync_point.h"
#include "util/dynamic_cache.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "util/trace.h"

namespace starrocks::lake {

namespace {

Status apply_alter_meta_log(TabletMetadataPB* metadata, const TxnLogPB_OpAlterMetadata& op_alter_metas,
                            TabletManager* tablet_mgr) {
    for (const auto& alter_meta : op_alter_metas.metadata_update_infos()) {
        if (alter_meta.has_enable_persistent_index()) {
            auto update_mgr = tablet_mgr->update_mgr();
            metadata->set_enable_persistent_index(alter_meta.enable_persistent_index());
            update_mgr->set_enable_persistent_index(metadata->id(), alter_meta.enable_persistent_index());
            // Try remove index from index cache
            // If tablet is doing apply rowset right now, remove primary index from index cache may be failed
            // because the primary index is available in cache
            // But it will be remove from index cache after apply is finished
            (void)update_mgr->index_cache().try_remove_by_key(metadata->id());
        }
        // Check if the alter_meta has a persistent index type change
        if (alter_meta.has_persistent_index_type()) {
            // Get the previous and new persistent index types
            PersistentIndexTypePB prev_type = metadata->persistent_index_type();
            PersistentIndexTypePB new_type = alter_meta.persistent_index_type();
            // Apply the changes to the persistent index type
            metadata->set_persistent_index_type(new_type);
            LOG(INFO) << fmt::format("alter persistent index type from {} to {} for tablet id: {}",
                                     PersistentIndexTypePB_Name(prev_type), PersistentIndexTypePB_Name(new_type),
                                     metadata->id());
            // Get the update manager
            auto update_mgr = tablet_mgr->update_mgr();
            // Try to remove the index from the index cache
            (void)update_mgr->index_cache().try_remove_by_key(metadata->id());
        }
        // update tablet meta
        // 1. rowset_to_schema is empty, maybe upgrade from old version or first time to do fast ddl. So we will
        //    add the tablet schema before alter into historical schema.
        // 2. rowset_to_schema is not empty, no need to update historical schema because we historical schema already
        //    keep the tablet schema before alter.
        if (alter_meta.has_tablet_schema()) {
            VLOG(2) << "old schema: " << metadata->schema().DebugString()
                    << " new schema: " << alter_meta.tablet_schema().DebugString();
            // add/drop field for struct column is under testing, To avoid impacting the existing logic, add the
            // `lake_enable_alter_struct` configuration. Once testing is complete, this configuration will be removed.
            if (config::lake_enable_alter_struct) {
                if (metadata->rowset_to_schema().empty() && metadata->rowsets_size() > 0) {
                    metadata->mutable_historical_schemas()->clear();
                    auto schema_id = metadata->schema().id();
                    auto& item = (*metadata->mutable_historical_schemas())[schema_id];
                    item.CopyFrom(metadata->schema());
                    for (int i = 0; i < metadata->rowsets_size(); i++) {
                        (*metadata->mutable_rowset_to_schema())[metadata->rowsets(i).id()] = schema_id;
                    }
                }
                // no need to update
            }
            metadata->mutable_schema()->CopyFrom(alter_meta.tablet_schema());
        }

        if (alter_meta.has_bundle_tablet_metadata()) {
            // do nothing
        }

        if (alter_meta.has_compaction_strategy()) {
            LOG(INFO) << fmt::format("alter compaction strategy from {} to {} for tablet id: {}",
                                     CompactionStrategyPB_Name(metadata->compaction_strategy()),
                                     CompactionStrategyPB_Name(alter_meta.compaction_strategy()), metadata->id());
            metadata->set_compaction_strategy(alter_meta.compaction_strategy());
        }
    }
    return Status::OK();
}

/**
 * @brief Updates the tablet metadata with the latest schema from the transaction log.
 * 
 * If the write operation contains a newer schema version than the current tablet schema,
 * this function attempts to fetch the new schema via TableSchemaService and update
 * the tablet metadata. It also archives the old schema into the historical schemas list.
 * 
 * @param op_write The write operation from the transaction log.
 * @param txn_id The transaction ID.
 * @param tablet_meta Pointer to the mutable tablet metadata to be updated.
 * @param tablet_mgr Pointer to the tablet manager.
 * @return Status::OK() on success or if no update is needed, otherwise an error status.
 */
Status update_metadata_schema(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                              const MutableTabletMetadataPtr& tablet_meta, TabletManager* tablet_mgr) {
    if (!op_write.has_schema_key()) {
        // not fast schema evolution v2, skip to update
        return Status::OK();
    }
    auto& schema_key = op_write.schema_key();
    if (schema_key.schema_id() == tablet_meta->schema().id() ||
        tablet_meta->historical_schemas().contains(schema_key.schema_id())) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto new_schema, tablet_mgr->table_schema_service()->get_schema_for_load(
                                              schema_key, tablet_meta->id(), txn_id, tablet_meta));
    auto& old_schema = tablet_meta->schema();
    if (new_schema->schema_version() <= old_schema.schema_version()) {
        return Status::OK();
    }

    LOG(INFO) << "update metadata schema. db_id: " << schema_key.db_id() << ", table_id: " << schema_key.table_id()
              << ", tablet_id: " << tablet_meta->id() << ", metadata version: " << tablet_meta->version()
              << ", txn_id: " << txn_id << ", new schema id/version: " << new_schema->id() << "/"
              << new_schema->schema_version() << ", old schema id/version: " << old_schema.id() << "/"
              << old_schema.schema_version();

    bool record_old_schema_in_history = false;
    for (auto& rowset : tablet_meta->rowsets()) {
        if (tablet_meta->rowset_to_schema().count(rowset.id()) <= 0) {
            record_old_schema_in_history = true;
            tablet_meta->mutable_rowset_to_schema()->insert({rowset.id(), old_schema.id()});
        }
    }
    if (record_old_schema_in_history && tablet_meta->historical_schemas().count(old_schema.id()) <= 0) {
        auto& item = (*tablet_meta->mutable_historical_schemas())[old_schema.id()];
        item.CopyFrom(old_schema);
    }
    tablet_meta->mutable_schema()->Clear();
    new_schema->to_schema_pb(tablet_meta->mutable_schema());
    return Status::OK();
}

} // namespace

class PrimaryKeyTxnLogApplier : public TxnLogApplier {
public:
    PrimaryKeyTxnLogApplier(const Tablet& tablet, MutableTabletMetadataPtr metadata, int64_t new_version,
                            bool rebuild_pindex, bool skip_write_tablet_metadata)
            : _tablet(tablet),
              _metadata(std::move(metadata)),
              _base_version(_metadata->version()),
              _new_version(new_version),
              _builder(_tablet, _metadata),
              _rebuild_pindex(rebuild_pindex) {
        _metadata->set_version(_new_version);
        _skip_write_tablet_metadata = skip_write_tablet_metadata;
    }

    ~PrimaryKeyTxnLogApplier() override { handle_failure(); }

    Status init() override { return check_meta_version(); }

    Status check_meta_version() {
        // check tablet meta
        RETURN_IF_ERROR(_tablet.update_mgr()->check_meta_version(_tablet, _base_version));
        return Status::OK();
    }

    void handle_failure() {
        if (_index_entry != nullptr && !_has_finalized) {
            // if we meet failures and have not finalized yet, have to clear primary index,
            // then we can retry again.
            // 1. unload index first
            _index_entry->value().unload();
            // 2. and then release guard
            _guard.reset(nullptr);
            // 3. remove index from cache to save resource
            _tablet.update_mgr()->remove_primary_index_cache(_index_entry);
        } else {
            _tablet.update_mgr()->release_primary_index_cache(_index_entry);
        }
        _index_entry = nullptr;
    }

    Status check_rebuild_index() {
        if (_rebuild_pindex) {
            for (const auto& sstable : _metadata->sstable_meta().sstables()) {
                FileMetaPB file_meta;
                file_meta.set_name(sstable.filename());
                file_meta.set_size(sstable.filesize());
                file_meta.set_shared(sstable.shared());
                _metadata->mutable_orphan_files()->Add(std::move(file_meta));
            }
            _metadata->clear_sstable_meta();
            _guard.reset(nullptr);
            _index_entry = nullptr;
            ASSIGN_OR_RETURN(_index_entry, _tablet.update_mgr()->rebuild_primary_index(
                                                   _metadata, &_builder, _base_version, _new_version, _guard));
            _rebuild_pindex = false;
        }
        return Status::OK();
    }

    Status apply(const TxnLogPB& log) override {
        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(true);
        SCOPED_THREAD_LOCAL_SINGLETON_CHECK_MEM_TRACKER_SETTER(
                config::enable_pk_strict_memcheck ? _tablet.update_mgr()->mem_tracker() : nullptr);
        _max_txn_id = std::max(_max_txn_id, log.txn_id());
        RETURN_IF_ERROR(check_rebuild_index());
        if (log.has_op_write()) {
            RETURN_IF_ERROR(check_and_recover([&]() { return apply_write_log(log.op_write(), log.txn_id()); }));
        }
        if (log.has_op_compaction()) {
            RETURN_IF_ERROR(
                    check_and_recover([&]() { return apply_compaction_log(log.op_compaction(), log.txn_id()); }));
        }
        if (log.has_op_schema_change()) {
            RETURN_IF_ERROR(apply_schema_change_log(log.op_schema_change()));
        }
        if (log.has_op_alter_metadata()) {
            DCHECK_EQ(_base_version + 1, _new_version);
            return apply_alter_meta_log(_metadata.get(), log.op_alter_metadata(), _tablet.tablet_mgr());
        }
        if (log.has_op_replication()) {
            RETURN_IF_ERROR(apply_replication_log(log.op_replication(), log.txn_id()));
        }
        return Status::OK();
    }

    Status apply(const TxnLogVector& txn_logs) override {
        if (txn_logs.empty()) {
            return Status::OK();
        }

        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(true);
        SCOPED_THREAD_LOCAL_SINGLETON_CHECK_MEM_TRACKER_SETTER(
                config::enable_pk_strict_memcheck ? _tablet.update_mgr()->mem_tracker() : nullptr);

        // Pre-check: only accept op_write operations
        for (const auto& log : txn_logs) {
            _max_txn_id = std::max(_max_txn_id, log->txn_id());

            // Ensure this log has op_write
            if (!log->has_op_write()) {
                return Status::NotSupported(
                        "Transaction log without op_write operation is not supported in batch apply");
            }
        }
        RETURN_IF_ERROR(check_rebuild_index());

        // At this point, all logs contain only op_write operations
        _tablet.update_mgr()->lock_shard_pk_index_shard(_tablet.id());
        DeferOp defer([&]() { _tablet.update_mgr()->unlock_shard_pk_index_shard(_tablet.id()); });

        RETURN_IF_ERROR(prepare_primary_index());

        // Collect all write operations data
        for (const auto& log : txn_logs) {
            if (log->has_op_write()) {
                const auto& op_write = log->op_write();
                RETURN_IF_ERROR(update_metadata_schema(op_write, log->txn_id(), _metadata, _tablet.tablet_mgr()));
                if (is_column_mode_partial_update(op_write)) {
                    RETURN_IF_ERROR(_tablet.update_mgr()->publish_column_mode_partial_update(
                            op_write, log->txn_id(), _metadata, &_tablet, _index_entry, &_builder, _base_version));
                } else {
                    RETURN_IF_ERROR(_tablet.update_mgr()->publish_primary_key_tablet(op_write, log->txn_id(), _metadata,
                                                                                     &_tablet, _index_entry, &_builder,
                                                                                     _base_version, true));
                }
            }
        }
        _builder.set_final_rowset();

        return Status::OK();
    }

    Status finish() override {
        SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(true);
        SCOPED_THREAD_LOCAL_SINGLETON_CHECK_MEM_TRACKER_SETTER(
                config::enable_pk_strict_memcheck ? _tablet.update_mgr()->mem_tracker() : nullptr);
        // local persistent index will update index version, so we need to load first
        // still need prepre primary index even there is an empty compaction
        if (_index_entry == nullptr &&
            (_has_empty_compaction || (_metadata->enable_persistent_index() &&
                                       _metadata->persistent_index_type() == PersistentIndexTypePB::LOCAL))) {
            // get lock to avoid gc
            _tablet.update_mgr()->lock_shard_pk_index_shard(_tablet.id());
            DeferOp defer([&]() { _tablet.update_mgr()->unlock_shard_pk_index_shard(_tablet.id()); });
            RETURN_IF_ERROR(prepare_primary_index());
        }

        // Must call `commit` before `finalize`,
        // because if `commit` or `finalize` fail, we can remove index in `handle_failure`.
        // if `_index_entry` is null, do nothing.
        if (_index_entry != nullptr) {
            RETURN_IF_ERROR(_index_entry->value().commit(_metadata, &_builder));
            _tablet.update_mgr()->index_cache().update_object_size(_index_entry, _index_entry->value().memory_usage());
        }
        _metadata->GetReflection()->MutableUnknownFields(_metadata.get())->Clear();
        RETURN_IF_ERROR(_builder.finalize(_max_txn_id, _skip_write_tablet_metadata));
        _has_finalized = true;
        return Status::OK();
    }

private:
    bool need_recover(const Status& st) { return _builder.recover_flag() != RecoverFlag::OK; }
    bool need_re_publish(const Status& st) { return _builder.recover_flag() == RecoverFlag::RECOVER_WITH_PUBLISH; }
    bool is_column_mode_partial_update(const TxnLogPB_OpWrite& op_write) const {
        auto mode = op_write.txn_meta().partial_update_mode();
        return mode == PartialUpdateMode::COLUMN_UPDATE_MODE || mode == PartialUpdateMode::COLUMN_UPSERT_MODE;
    }

    Status check_and_recover(const std::function<Status()>& publish_func) {
        auto ret = publish_func();
        if (config::enable_primary_key_recover && need_recover(ret)) {
            {
                TRACE_COUNTER_SCOPE_LATENCY_US("primary_key_recover");
                LOG(INFO) << "Primary Key recover begin, tablet_id: " << _tablet.id() << " base_ver: " << _base_version;
                // release and remove index entry's reference
                _tablet.update_mgr()->release_primary_index_cache(_index_entry);
                _guard.reset(nullptr);
                _index_entry = nullptr;
                // rebuild delvec and pk index
                LakePrimaryKeyRecover recover(&_builder, &_tablet, _metadata);
                RETURN_IF_ERROR(recover.recover());
                LOG(INFO) << "Primary Key recover finish, tablet_id: " << _tablet.id()
                          << " base_ver: " << _base_version;
            }
            if (need_re_publish(ret)) {
                _builder.set_recover_flag(RecoverFlag::OK);
                // duplicate primary key happen when prepare index, so we need to re-publish it.
                return publish_func();
            } else {
                _builder.set_recover_flag(RecoverFlag::OK);
                // No need to re-publish, make sure txn log already apply
                return Status::OK();
            }
        }
        return ret;
    }

    // We call `prepare_primary_index` only when first time we apply `write_log` or `compaction_log`, instead of
    // in `TxnLogApplier.init`, because we have to build primary index after apply `schema_change_log` finish.
    Status prepare_primary_index() {
        if (_index_entry == nullptr) {
            ASSIGN_OR_RETURN(_index_entry, _tablet.update_mgr()->prepare_primary_index(
                                                   _metadata, &_builder, _base_version, _new_version, _guard));
        }
        return Status::OK();
    }

    Status apply_write_log(const TxnLogPB_OpWrite& op_write, int64_t txn_id) {
        RETURN_IF_ERROR(update_metadata_schema(op_write, txn_id, _metadata, _tablet.tablet_mgr()));
        // get lock to avoid gc
        _tablet.update_mgr()->lock_shard_pk_index_shard(_tablet.id());
        DeferOp defer([&]() { _tablet.update_mgr()->unlock_shard_pk_index_shard(_tablet.id()); });

        if (op_write.dels_size() == 0 && op_write.rowset().num_rows() == 0 &&
            !op_write.rowset().has_delete_predicate()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(prepare_primary_index());
        if (is_column_mode_partial_update(op_write)) {
            return _tablet.update_mgr()->publish_column_mode_partial_update(op_write, txn_id, _metadata, &_tablet,
                                                                            _index_entry, &_builder, _base_version);
        } else {
            return _tablet.update_mgr()->publish_primary_key_tablet(op_write, txn_id, _metadata, &_tablet, _index_entry,
                                                                    &_builder, _base_version);
        }
    }

    Status apply_compaction_log(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id) {
        // get lock to avoid gc
        _tablet.update_mgr()->lock_shard_pk_index_shard(_tablet.id());
        DeferOp defer([&]() { _tablet.update_mgr()->unlock_shard_pk_index_shard(_tablet.id()); });

        // New format: subtask_outputs contains independent outputs for each subtask
        // Check this FIRST before checking input_rowsets().empty(), because in parallel compaction mode
        // the merged txn_log only has subtask_outputs populated, not input_rowsets directly.
        if (!op_compaction.subtask_outputs().empty()) {
            RETURN_IF_ERROR(prepare_primary_index());
            return _tablet.update_mgr()->publish_primary_compaction_multi_output(
                    op_compaction, txn_id, _metadata, _tablet, _index_entry, &_builder, _base_version);
        }

        if (op_compaction.input_rowsets().empty()) {
            DCHECK(!op_compaction.has_output_rowset() || op_compaction.output_rowset().num_rows() == 0);
            // Apply the compaction operation to the cloud native pk index.
            // This ensures that the pk index is updated with the compaction changes.
            _builder.remove_compacted_sst(op_compaction);
            if (op_compaction.input_sstables().empty()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(prepare_primary_index());
            RETURN_IF_ERROR(_index_entry->value().apply_opcompaction(*_metadata, op_compaction));
            return Status::OK();
        }
        RETURN_IF_ERROR(prepare_primary_index());

        // Legacy format: single merged output_rowset
        return _tablet.update_mgr()->publish_primary_compaction(op_compaction, txn_id, *_metadata, _tablet,
                                                                _index_entry, &_builder, _base_version);
    }

    Status apply_schema_change_log(const TxnLogPB_OpSchemaChange& op_schema_change) {
        DCHECK_EQ(1, _base_version);
        DCHECK_EQ(0, _metadata->rowsets_size());
        for (const auto& rowset : op_schema_change.rowsets()) {
            DCHECK(rowset.has_id());
            auto new_rowset = _metadata->add_rowsets();
            new_rowset->CopyFrom(rowset);
            _metadata->set_next_rowset_id(new_rowset->id() + std::max(1, new_rowset->segments_size()));
        }
        if (op_schema_change.has_delvec_meta()) {
            DCHECK(op_schema_change.linked_segment());
            _metadata->mutable_delvec_meta()->CopyFrom(op_schema_change.delvec_meta());
        }
        // op_schema_change.alter_version() + 1 < _new_version means there are other logs to apply besides the current
        // schema change log.
        if (op_schema_change.alter_version() + 1 < _new_version) {
            // Save metadata before applying other transaction logs, don't bother to update primary index and
            // load delete vector here.
            _base_version = op_schema_change.alter_version();
            auto base_meta = std::make_shared<TabletMetadata>(*_metadata);
            base_meta->set_version(_base_version);
            RETURN_IF_ERROR(_tablet.put_metadata(std::move(base_meta)));
        }
        return Status::OK();
    }

    Status apply_replication_log(const TxnLogPB_OpReplication& op_replication, int64_t txn_id) {
        const auto& txn_meta = op_replication.txn_meta();

        if (txn_meta.txn_state() != ReplicationTxnStatePB::TXN_REPLICATED) {
            LOG(WARNING) << "Fail to apply replication log, invalid txn meta state: "
                         << ReplicationTxnStatePB_Name(txn_meta.txn_state());
            return Status::Corruption("Invalid txn meta state: " + ReplicationTxnStatePB_Name(txn_meta.txn_state()));
        }

        if (txn_meta.data_version() == 0) {
            if (txn_meta.snapshot_version() != _new_version) {
                LOG(WARNING) << "Fail to apply replication log, mismatched snapshot version and new version"
                             << ", snapshot version: " << txn_meta.snapshot_version()
                             << ", base version: " << txn_meta.visible_version() << ", new version: " << _new_version;
                return Status::Corruption("mismatched snapshot version and new version");
            }
        } else if (txn_meta.snapshot_version() - txn_meta.data_version() + _base_version != _new_version) {
            LOG(WARNING) << "Fail to apply replication log, mismatched version, snapshot version: "
                         << txn_meta.snapshot_version() << ", data version: " << txn_meta.data_version()
                         << ", base version: " << _base_version << ", new version: " << _new_version;
            return Status::Corruption("mismatched version");
        }

        if (txn_meta.incremental_snapshot()) {
            for (const auto& op_write : op_replication.op_writes()) {
                RETURN_IF_ERROR(apply_write_log(op_write, txn_id));
            }
            LOG(INFO) << "Apply pk incremental replication log finish. tablet_id: " << _tablet.id()
                      << ", base_version: " << _base_version << ", new_version: " << _new_version
                      << ", txn_id: " << txn_id;
        } else {
            if (op_replication.has_tablet_metadata()) {
                // Same logic for pk and non-pk tables
                auto old_rowsets = std::move(*_metadata->mutable_rowsets());

                const auto& copied_tablet_meta = op_replication.tablet_metadata();
                if (copied_tablet_meta.rowsets_size() > 0) {
                    _metadata->mutable_rowsets()->Clear();
                    _metadata->mutable_rowsets()->CopyFrom(copied_tablet_meta.rowsets());
                }

                if (copied_tablet_meta.has_dcg_meta()) {
                    _metadata->mutable_dcg_meta()->Clear();
                    _metadata->mutable_dcg_meta()->CopyFrom(copied_tablet_meta.dcg_meta());
                }

                if (copied_tablet_meta.has_sstable_meta()) {
                    _metadata->mutable_sstable_meta()->Clear();
                    _metadata->mutable_sstable_meta()->CopyFrom(copied_tablet_meta.sstable_meta());
                }

                if (copied_tablet_meta.has_delvec_meta()) {
                    _metadata->mutable_delvec_meta()->Clear();
                    _metadata->mutable_delvec_meta()->CopyFrom(copied_tablet_meta.delvec_meta());
                }

                _metadata->set_next_rowset_id(copied_tablet_meta.next_rowset_id());
                _metadata->set_cumulative_point(0);
                old_rowsets.Swap(_metadata->mutable_compaction_inputs());

                _tablet.update_mgr()->unload_primary_index(_tablet.id());

                VLOG(3) << "Apply pk replication log with tablet metadata provided. tablet_id: " << _tablet.id()
                        << ", base_version: " << _base_version << ", new_version: " << _new_version
                        << ", txn_id: " << txn_meta.txn_id() << ", metadata id: " << _metadata->id()
                        << ", next_rowset_id: " << _metadata->next_rowset_id()
                        << ", rowsets size: " << _metadata->rowsets_size();
            } else {
                auto old_rowsets = std::move(*_metadata->mutable_rowsets());
                _metadata->mutable_rowsets()->Clear();
                _metadata->mutable_delvec_meta()->Clear();

                auto new_next_rowset_id = _metadata->next_rowset_id();
                for (const auto& op_write : op_replication.op_writes()) {
                    auto rowset = _metadata->add_rowsets();
                    rowset->CopyFrom(op_write.rowset());
                    const auto new_rowset_id = rowset->id() + _metadata->next_rowset_id();
                    rowset->set_id(new_rowset_id);
                    new_next_rowset_id = std::max<uint32_t>(new_next_rowset_id,
                                                            new_rowset_id + std::max(1, rowset->segments_size()));
                }

                for (const auto& [segment_id, delvec_data] : op_replication.delvecs()) {
                    auto delvec = std::make_shared<DelVector>();
                    RETURN_IF_ERROR(delvec->load(_new_version, delvec_data.data().data(), delvec_data.data().size()));
                    _builder.append_delvec(delvec, segment_id + _metadata->next_rowset_id());
                }

                _metadata->set_next_rowset_id(new_next_rowset_id);
                _metadata->set_cumulative_point(0);
                old_rowsets.Swap(_metadata->mutable_compaction_inputs());

                _tablet.update_mgr()->unload_primary_index(_tablet.id());
            }

            LOG(INFO) << "Apply pk full replication log finish. tablet_id: " << _tablet.id()
                      << ", base_version: " << _base_version << ", new_version: " << _new_version
                      << ", txn_id: " << txn_id;
        }

        if (op_replication.has_source_schema()) {
            _metadata->mutable_source_schema()->CopyFrom(op_replication.source_schema());
        }

        return Status::OK();
    }

    Tablet _tablet;
    MutableTabletMetadataPtr _metadata;
    int64_t _base_version{0};
    int64_t _new_version{0};
    int64_t _max_txn_id{0}; // Used as the file name prefix of the delvec file
    MetaFileBuilder _builder;
    DynamicCache<uint64_t, LakePrimaryIndex>::Entry* _index_entry{nullptr};
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> _guard{nullptr};
    // True when finalize meta file success.
    bool _has_finalized = false;
    bool _rebuild_pindex = false;
};

class NonPrimaryKeyTxnLogApplier : public TxnLogApplier {
public:
    NonPrimaryKeyTxnLogApplier(const Tablet& tablet, MutableTabletMetadataPtr metadata, int64_t new_version,
                               bool skip_write_tablet_metadata)
            : _tablet(tablet), _metadata(std::move(metadata)), _new_version(new_version) {
        _skip_write_tablet_metadata = skip_write_tablet_metadata;
    }

    Status apply(const TxnLogPB& log) override {
        if (log.has_op_write()) {
            RETURN_IF_ERROR(apply_write_log(log.op_write(), log.txn_id()));
        }
        if (log.has_op_compaction()) {
            RETURN_IF_ERROR(apply_compaction_log(log.op_compaction()));
        }
        if (log.has_op_schema_change()) {
            RETURN_IF_ERROR(apply_schema_change_log(log.op_schema_change()));
        }
        if (log.has_op_replication()) {
            RETURN_IF_ERROR(apply_replication_log(log.op_replication()));
        }
        if (log.has_op_alter_metadata()) {
            return apply_alter_meta_log(_metadata.get(), log.op_alter_metadata(), _tablet.tablet_mgr());
        }
        return Status::OK();
    }

    Status apply(const TxnLogVector& txn_logs) override {
        if (txn_logs.empty()) {
            return Status::OK();
        }

        VLOG(2) << "Applying " << txn_logs.size() << " transaction logs in batch for tablet " << _tablet.id();

        // Collect all rowset information to be merged
        int64_t total_num_rows = 0;
        int64_t total_data_size = 0;
        std::vector<std::string> all_segments;
        std::vector<int64_t> all_segment_sizes;
        std::vector<std::string> all_segment_encryption_metas;
        std::vector<const SegmentMetadataPB*> all_segment_metas;

        // Traverse all transaction logs and collect op_write information
        VLOG(2) << "Collecting op_write information from transaction logs for tablet " << _tablet.id();
        for (const auto& log : txn_logs) {
            if (log->has_op_write()) {
                const auto& op_write = log->op_write();
                RETURN_IF_ERROR(update_metadata_schema(op_write, log->txn_id(), _metadata, _tablet.tablet_mgr()));
                if (op_write.has_rowset() && op_write.rowset().num_rows() > 0) {
                    const auto& rowset = op_write.rowset();

                    // Check for delete predicate - not supported in batch mode
                    if (rowset.has_delete_predicate()) {
                        LOG(WARNING) << "Delete predicate is not supported in batch transaction log apply for tablet "
                                     << _tablet.id();
                        return Status::NotSupported("Delete predicate is not supported in batch transaction log apply");
                    }

                    // Accumulate row count and data size
                    total_num_rows += rowset.num_rows();
                    total_data_size += rowset.data_size();

                    // Collect all segments
                    all_segments.reserve(all_segments.size() + rowset.segments_size());
                    for (int i = 0; i < rowset.segments_size(); i++) {
                        all_segments.emplace_back(rowset.segments(i));
                    }

                    // Collect segment sizes
                    all_segment_sizes.reserve(all_segment_sizes.size() + rowset.segment_size_size());
                    for (int i = 0; i < rowset.segment_size_size(); i++) {
                        all_segment_sizes.emplace_back(rowset.segment_size(i));
                    }

                    // Collect encryption metas directly as strings
                    all_segment_encryption_metas.reserve(all_segment_encryption_metas.size() +
                                                         rowset.segment_encryption_metas_size());
                    for (int i = 0; i < rowset.segment_encryption_metas_size(); i++) {
                        all_segment_encryption_metas.emplace_back(rowset.segment_encryption_metas(i));
                    }

                    // Collect segment metas
                    all_segment_metas.reserve(all_segment_metas.size() + rowset.segment_metas_size());
                    for (int i = 0; i < rowset.segment_metas_size(); i++) {
                        all_segment_metas.emplace_back(&rowset.segment_metas(i));
                    }
                }
            } else {
                return Status::NotSupported(
                        "Transaction log without op_write operation is not supported in batch apply");
            }
        }

        // If no valid rowset data, return directly
        if (total_num_rows == 0) {
            VLOG(2) << "No valid rowset data to apply for tablet " << _tablet.id();
            return Status::OK();
        }

        VLOG(2) << "Creating merged rowset with total_num_rows=" << total_num_rows
                << ", total_data_size=" << total_data_size << " for tablet " << _tablet.id();
        // Create merged rowset
        auto merged_rowset = _metadata->add_rowsets();
        merged_rowset->set_num_rows(total_num_rows);
        merged_rowset->set_data_size(total_data_size);
        merged_rowset->set_overlapped(all_segments.size() > 1);

        // Set segments using move semantics
        for (auto&& segment : std::move(all_segments)) {
            merged_rowset->add_segments(std::move(segment));
        }

        // Set segment sizes
        for (int64_t size : all_segment_sizes) {
            merged_rowset->add_segment_size(size);
        }

        // Set segment encryption metas directly
        for (const auto& meta : all_segment_encryption_metas) {
            merged_rowset->add_segment_encryption_metas(meta);
        }

        // Set segment metas
        for (const auto* segment_meta : all_segment_metas) {
            merged_rowset->add_segment_metas()->CopyFrom(*segment_meta);
        }

        // Set rowset ID and update next_rowset_id
        merged_rowset->set_id(_metadata->next_rowset_id());
        _metadata->set_next_rowset_id(_metadata->next_rowset_id() + std::max(1, merged_rowset->segments_size()));
        VLOG(2) << "Set rowset id to " << merged_rowset->id() << " and updated next_rowset_id to "
                << _metadata->next_rowset_id() << " for tablet " << _tablet.id();

        // Update schema related information
        if (!_metadata->rowset_to_schema().empty()) {
            auto schema_id = _metadata->schema().id();
            (*_metadata->mutable_rowset_to_schema())[merged_rowset->id()] = schema_id;

            // Add to historical schema if it's the first rowset of latest schema
            if (_metadata->historical_schemas().count(schema_id) <= 0) {
                VLOG(2) << "Adding new schema " << schema_id << " to historical schemas for tablet " << _tablet.id();
                auto& item = (*_metadata->mutable_historical_schemas())[schema_id];
                item.CopyFrom(_metadata->schema());
            }
        }

        return Status::OK();
    }

    Status finish() override {
        _metadata->GetReflection()->MutableUnknownFields(_metadata.get())->Clear();
        _metadata->set_version(_new_version);
        if (_skip_write_tablet_metadata) {
            return ExecEnv::GetInstance()->lake_tablet_manager()->cache_tablet_metadata(_metadata);
        }
        return _tablet.put_metadata(_metadata);
    }

private:
    Status apply_write_log(const TxnLogPB_OpWrite& op_write, int64_t txn_id) {
        TEST_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_write_log");
        RETURN_IF_ERROR(update_metadata_schema(op_write, txn_id, _metadata, _tablet.tablet_mgr()));
        if (op_write.has_rowset() && (op_write.rowset().num_rows() > 0 || op_write.rowset().has_delete_predicate())) {
            auto rowset = _metadata->add_rowsets();
            rowset->CopyFrom(op_write.rowset());
            rowset->set_id(_metadata->next_rowset_id());
            _metadata->set_next_rowset_id(_metadata->next_rowset_id() + std::max(1, rowset->segments_size()));
            if (!_metadata->rowset_to_schema().empty()) {
                auto schema_id = _metadata->schema().id();
                (*_metadata->mutable_rowset_to_schema())[rowset->id()] = schema_id;
                // first rowset of latest schema
                if (_metadata->historical_schemas().count(schema_id) <= 0) {
                    auto& item = (*_metadata->mutable_historical_schemas())[schema_id];
                    item.CopyFrom(_metadata->schema());
                }
            }
        }
        return Status::OK();
    }

    Status apply_compaction_log(const TxnLogPB_OpCompaction& op_compaction) {
        // New format: subtask_outputs contains independent outputs for each subtask
        // Each subtask's output replaces its corresponding input rowsets
        if (!op_compaction.subtask_outputs().empty()) {
            return apply_compaction_log_multi_output(op_compaction);
        }

        // Legacy format: single merged output_rowset replaces all input_rowsets
        return apply_compaction_log_single_output(op_compaction);
    }

    // Apply compaction with multiple independent outputs (new format for parallel compaction)
    // Each subtask's output rowset replaces its corresponding input rowsets
    Status apply_compaction_log_multi_output(const TxnLogPB_OpCompaction& op_compaction) {
        struct Finder {
            int64_t id;
            bool operator()(const RowsetMetadata& r) const { return r.id() == id; }
        };

        // Track total input/output counts for cumulative point calculation
        int32_t total_inputs_removed = 0;
        int32_t total_outputs_added = 0;
        int32_t min_first_idx = INT32_MAX;

        // Process each subtask output in order
        // Note: subtask_outputs are already sorted by subtask_id in get_merged_txn_log
        for (const auto& subtask_output : op_compaction.subtask_outputs()) {
            if (subtask_output.input_rowsets().empty()) {
                continue;
            }

            // Find the first input rowset position
            auto input_id = subtask_output.input_rowsets(0);
            auto first_input_pos = std::find_if(_metadata->mutable_rowsets()->begin(),
                                                _metadata->mutable_rowsets()->end(), Finder{input_id});
            if (UNLIKELY(first_input_pos == _metadata->mutable_rowsets()->end())) {
                LOG(WARNING) << "Subtask " << subtask_output.subtask_id() << " input rowset " << input_id
                             << " not found, skipping";
                continue;
            }

            // Verify all input rowsets exist and are adjacent
            auto pre_input_pos = first_input_pos;
            bool valid = true;
            for (int i = 1; i < subtask_output.input_rowsets_size(); i++) {
                input_id = subtask_output.input_rowsets(i);
                auto it = std::find_if(pre_input_pos + 1, _metadata->mutable_rowsets()->end(), Finder{input_id});
                if (it == _metadata->mutable_rowsets()->end()) {
                    LOG(WARNING) << "Subtask " << subtask_output.subtask_id() << " input rowset " << input_id
                                 << " not exist, skipping";
                    valid = false;
                    break;
                } else if (it != pre_input_pos + 1) {
                    LOG(WARNING) << "Subtask " << subtask_output.subtask_id() << " input rowset " << input_id
                                 << " not adjacent, skipping";
                    valid = false;
                    break;
                }
                pre_input_pos = it;
            }
            if (!valid) {
                continue;
            }

            // Get output rowset schema
            std::vector<uint32_t> input_rowsets_id(subtask_output.input_rowsets().begin(),
                                                   subtask_output.input_rowsets().end());
            ASSIGN_OR_RETURN(auto tablet_schema,
                             ExecEnv::GetInstance()->lake_tablet_manager()->get_output_rowset_schema(input_rowsets_id,
                                                                                                     _metadata.get()));
            int64_t output_rowset_schema_id = tablet_schema->id();

            auto first_idx = static_cast<uint32_t>(first_input_pos - _metadata->mutable_rowsets()->begin());
            min_first_idx = std::min(min_first_idx, static_cast<int32_t>(first_idx));

            // Move input rowsets to compaction_inputs
            const auto end_input_pos = pre_input_pos + 1;
            for (auto iter = first_input_pos; iter != end_input_pos; ++iter) {
                _metadata->mutable_compaction_inputs()->Add(std::move(*iter));
            }

            bool has_output_rowset = false;
            uint32_t output_rowset_id = 0;
            if (subtask_output.has_output_rowset() && subtask_output.output_rowset().num_rows() > 0) {
                // Replace the first input rowset with output rowset
                auto output_rowset = _metadata->mutable_rowsets(first_idx);
                output_rowset->CopyFrom(subtask_output.output_rowset());
                output_rowset->set_id(_metadata->next_rowset_id());
                _metadata->set_next_rowset_id(_metadata->next_rowset_id() + output_rowset->segments_size());
                ++first_input_pos;
                has_output_rowset = true;
                output_rowset_id = output_rowset->id();
                total_outputs_added++;
            }

            // Erase remaining input rowsets from _metadata
            int erased_count = end_input_pos - first_input_pos;
            _metadata->mutable_rowsets()->erase(first_input_pos, end_input_pos);
            total_inputs_removed += subtask_output.input_rowsets_size();

            // Update historical schema and rowset schema id
            if (!_metadata->rowset_to_schema().empty()) {
                for (int i = 0; i < subtask_output.input_rowsets_size(); i++) {
                    _metadata->mutable_rowset_to_schema()->erase(subtask_output.input_rowsets(i));
                }

                if (has_output_rowset) {
                    (*_metadata->mutable_rowset_to_schema())[output_rowset_id] = output_rowset_schema_id;
                }
            }

            VLOG(1) << "Applied subtask " << subtask_output.subtask_id()
                    << " output: inputs=" << subtask_output.input_rowsets_size() << ", has_output=" << has_output_rowset
                    << ", erased=" << erased_count;
        }

        // Clean up unused historical schemas
        if (!_metadata->rowset_to_schema().empty()) {
            std::unordered_set<int64_t> schema_id;
            for (auto& pair : _metadata->rowset_to_schema()) {
                schema_id.insert(pair.second);
            }
            for (auto it = _metadata->mutable_historical_schemas()->begin();
                 it != _metadata->mutable_historical_schemas()->end();) {
                if (schema_id.find(it->first) == schema_id.end()) {
                    it = _metadata->mutable_historical_schemas()->erase(it);
                } else {
                    it++;
                }
            }
        }

        // Set new cumulative point
        // size tiered compaction policy does not need cumulative point (set to 0)
        if (config::enable_size_tiered_compaction_strategy) {
            _metadata->set_cumulative_point(0);
        } else if (min_first_idx != INT32_MAX) {
            // Only update cumulative point when at least one subtask successfully found its input rowsets
            uint32_t new_cumulative_point = 0;
            if (static_cast<uint32_t>(min_first_idx) >= _metadata->cumulative_point()) {
                new_cumulative_point = min_first_idx;
            } else if (_metadata->cumulative_point() >= static_cast<uint32_t>(total_inputs_removed)) {
                new_cumulative_point = _metadata->cumulative_point() - total_inputs_removed;
            }
            new_cumulative_point += total_outputs_added;
            // Use DCHECK to catch logic bugs in debug mode, but clamp in release to avoid
            // inconsistent state (metadata already modified at this point)
            DCHECK_LE(new_cumulative_point, _metadata->rowsets_size())
                    << "new cumulative point: " << new_cumulative_point
                    << " exceeds rowset size: " << _metadata->rowsets_size();
            if (new_cumulative_point > _metadata->rowsets_size()) {
                LOG(ERROR) << "new cumulative point: " << new_cumulative_point
                           << " exceeds rowset size: " << _metadata->rowsets_size() << ", clamping to rowset size";
                new_cumulative_point = _metadata->rowsets_size();
            }
            _metadata->set_cumulative_point(new_cumulative_point);
        } else {
            // min_first_idx == INT32_MAX means no subtask found its input rowsets,
            // preserve the existing cumulative point unchanged (consistent with single-output early return)
            LOG(INFO) << "No subtask found input rowsets, preserving cumulative point: "
                      << _metadata->cumulative_point();
        }

        // Debug new tablet metadata
        std::vector<uint32_t> rowset_ids;
        std::vector<uint32_t> delete_rowset_ids;
        for (const auto& rowset : _metadata->rowsets()) {
            rowset_ids.emplace_back(rowset.id());
            if (rowset.has_delete_predicate()) {
                delete_rowset_ids.emplace_back(rowset.id());
            }
        }
        VLOG(1) << "Parallel compaction finish. tablet: " << _metadata->id() << ", version: " << _metadata->version()
                << ", cumulative point: " << _metadata->cumulative_point()
                << ", subtask_outputs: " << op_compaction.subtask_outputs_size() << ", rowsets: ["
                << JoinInts(rowset_ids, ",") << "]"
                << ", delete rowsets: [" << JoinInts(delete_rowset_ids, ",") + "]";
        return Status::OK();
    }

    // Apply compaction with single merged output (legacy format for backward compatibility)
    Status apply_compaction_log_single_output(const TxnLogPB_OpCompaction& op_compaction) {
        // It's ok to have a compaction log without input rowset and output rowset.
        if (op_compaction.input_rowsets().empty()) {
            DCHECK(!op_compaction.has_output_rowset() || op_compaction.output_rowset().num_rows() == 0);
            return Status::OK();
        }

        struct Finder {
            int64_t id;
            bool operator()(const RowsetMetadata& r) const { return r.id() == id; }
        };

        auto input_id = op_compaction.input_rowsets(0);
        auto first_input_pos = std::find_if(_metadata->mutable_rowsets()->begin(), _metadata->mutable_rowsets()->end(),
                                            Finder{input_id});
        if (UNLIKELY(first_input_pos == _metadata->mutable_rowsets()->end())) {
            LOG(INFO) << "input rowset not found";
            return Status::InternalError(fmt::format("input rowset {} not found", input_id));
        }

        // Safety check:
        // 1. All input rowsets must exist in |_metadata->rowsets()|
        // 2. Position of the input rowsets must be adjacent.
        auto pre_input_pos = first_input_pos;
        for (int i = 1, sz = op_compaction.input_rowsets_size(); i < sz; i++) {
            input_id = op_compaction.input_rowsets(i);
            auto it = std::find_if(pre_input_pos + 1, _metadata->mutable_rowsets()->end(), Finder{input_id});
            if (it == _metadata->mutable_rowsets()->end()) {
                return Status::InternalError(fmt::format("input rowset {} not exist", input_id));
            } else if (it != pre_input_pos + 1) {
                return Status::InternalError(fmt::format("input rowset position not adjacent"));
            } else {
                pre_input_pos = it;
            }
        }

        std::vector<uint32_t> input_rowsets_id(op_compaction.input_rowsets().begin(),
                                               op_compaction.input_rowsets().end());
        ASSIGN_OR_RETURN(auto tablet_schema, ExecEnv::GetInstance()->lake_tablet_manager()->get_output_rowset_schema(
                                                     input_rowsets_id, _metadata.get()));
        int64_t output_rowset_schema_id = tablet_schema->id();

        auto last_input_pos = pre_input_pos;
        RowsetMetadataPB last_input_rowset = *last_input_pos;
        trim_partial_compaction_last_input_rowset(_metadata, op_compaction, last_input_rowset);

        const auto end_input_pos = pre_input_pos + 1;
        for (auto iter = first_input_pos; iter != end_input_pos; ++iter) {
            if (iter != last_input_pos) {
                _metadata->mutable_compaction_inputs()->Add(std::move(*iter));
            } else {
                // might be a partial compaction, use real last input rowset
                _metadata->mutable_compaction_inputs()->Add(std::move(last_input_rowset));
            }
        }

        auto first_idx = static_cast<uint32_t>(first_input_pos - _metadata->mutable_rowsets()->begin());
        bool has_output_rowset = false;
        uint32_t output_rowset_id = 0;
        if (op_compaction.has_output_rowset() && op_compaction.output_rowset().num_rows() > 0) {
            // Replace the first input rowset with output rowset
            auto output_rowset = _metadata->mutable_rowsets(first_idx);
            output_rowset->CopyFrom(op_compaction.output_rowset());
            output_rowset->set_id(_metadata->next_rowset_id());
            _metadata->set_next_rowset_id(_metadata->next_rowset_id() + output_rowset->segments_size());
            ++first_input_pos;
            has_output_rowset = true;
            output_rowset_id = output_rowset->id();
        }
        // Erase input rowsets from _metadata
        _metadata->mutable_rowsets()->erase(first_input_pos, end_input_pos);

        // Update historical schema and rowset schema id
        if (!_metadata->rowset_to_schema().empty()) {
            for (int i = 0; i < op_compaction.input_rowsets_size(); i++) {
                _metadata->mutable_rowset_to_schema()->erase(op_compaction.input_rowsets(i));
            }

            if (has_output_rowset) {
                (*_metadata->mutable_rowset_to_schema())[output_rowset_id] = output_rowset_schema_id;
            }

            std::unordered_set<int64_t> schema_id;
            for (auto& pair : _metadata->rowset_to_schema()) {
                schema_id.insert(pair.second);
            }

            for (auto it = _metadata->mutable_historical_schemas()->begin();
                 it != _metadata->mutable_historical_schemas()->end();) {
                if (schema_id.find(it->first) == schema_id.end()) {
                    it = _metadata->mutable_historical_schemas()->erase(it);
                } else {
                    it++;
                }
            }
        }

        // Set new cumulative point
        uint32_t new_cumulative_point = 0;
        // size tiered compaction policy does not need cumulative point
        if (!config::enable_size_tiered_compaction_strategy) {
            if (first_idx >= _metadata->cumulative_point()) {
                // cumulative compaction
                new_cumulative_point = first_idx;
            } else if (_metadata->cumulative_point() >= op_compaction.input_rowsets_size()) {
                // base compaction
                new_cumulative_point = _metadata->cumulative_point() - op_compaction.input_rowsets_size();
            }
            if (op_compaction.has_output_rowset() && op_compaction.output_rowset().num_rows() > 0) {
                ++new_cumulative_point;
            }
            if (new_cumulative_point > _metadata->rowsets_size()) {
                return Status::InternalError(fmt::format("new cumulative point: {} exceeds rowset size: {}",
                                                         new_cumulative_point, _metadata->rowsets_size()));
            }
        }
        _metadata->set_cumulative_point(new_cumulative_point);

        // Debug new tablet metadata
        std::vector<uint32_t> rowset_ids;
        std::vector<uint32_t> delete_rowset_ids;
        for (const auto& rowset : _metadata->rowsets()) {
            rowset_ids.emplace_back(rowset.id());
            if (rowset.has_delete_predicate()) {
                delete_rowset_ids.emplace_back(rowset.id());
            }
        }
        LOG(INFO) << "Compaction finish. tablet: " << _metadata->id() << ", version: " << _metadata->version()
                  << ", cumulative point: " << _metadata->cumulative_point() << ", rowsets: ["
                  << JoinInts(rowset_ids, ",") << "]"
                  << ", delete rowsets: [" << JoinInts(delete_rowset_ids, ",") + "]";
        return Status::OK();
    }

    Status apply_schema_change_log(const TxnLogPB_OpSchemaChange& op_schema_change) {
        TEST_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_schema_change_log");
        DCHECK_EQ(0, _metadata->rowsets_size());
        for (const auto& rowset : op_schema_change.rowsets()) {
            DCHECK(rowset.has_id());
            auto new_rowset = _metadata->add_rowsets();
            new_rowset->CopyFrom(rowset);
            _metadata->set_next_rowset_id(new_rowset->id() + std::max(1, new_rowset->segments_size()));
        }
        DCHECK(!op_schema_change.has_delvec_meta());
        return Status::OK();
    }

    Status apply_replication_log(const TxnLogPB_OpReplication& op_replication) {
        const auto& txn_meta = op_replication.txn_meta();

        if (txn_meta.txn_state() != ReplicationTxnStatePB::TXN_REPLICATED) {
            LOG(WARNING) << "Fail to apply replication log, invalid txn meta state: "
                         << ReplicationTxnStatePB_Name(txn_meta.txn_state());
            return Status::Corruption("Invalid txn meta state: " + ReplicationTxnStatePB_Name(txn_meta.txn_state()));
        }

        if (txn_meta.data_version() == 0) {
            if (txn_meta.snapshot_version() != _new_version) {
                LOG(WARNING) << "Fail to apply replication log, mismatched snapshot version and new version"
                             << ", snapshot version: " << txn_meta.snapshot_version()
                             << ", base version: " << txn_meta.visible_version() << ", new version: " << _new_version;
                return Status::Corruption("mismatched snapshot version and new version");
            }
        } else if (txn_meta.snapshot_version() - txn_meta.data_version() + _metadata->version() != _new_version) {
            LOG(WARNING) << "Fail to apply replication log, mismatched version, snapshot version: "
                         << txn_meta.snapshot_version() << ", data version: " << txn_meta.data_version()
                         << ", base version: " << _metadata->version() << ", new version: " << _new_version;
            return Status::Corruption("mismatched version");
        }

        int64_t base_version = _metadata->version();
        if (txn_meta.incremental_snapshot()) {
            for (const auto& op_write : op_replication.op_writes()) {
                RETURN_IF_ERROR(apply_write_log(op_write, txn_meta.txn_id()));
            }
            LOG(INFO) << "Apply incremental replication log finish. tablet_id: " << _tablet.id()
                      << ", base_version: " << base_version << ", new_version: " << _new_version
                      << ", txn_id: " << txn_meta.txn_id();
        } else {
            if (op_replication.has_tablet_metadata()) {
                // Same logic for pk and non-pk tables
                auto old_rowsets = std::move(*_metadata->mutable_rowsets());

                const auto& copied_tablet_meta = op_replication.tablet_metadata();
                if (copied_tablet_meta.rowsets_size() > 0) {
                    _metadata->mutable_rowsets()->Clear();
                    _metadata->mutable_rowsets()->CopyFrom(copied_tablet_meta.rowsets());
                }

                if (copied_tablet_meta.has_dcg_meta()) {
                    _metadata->mutable_dcg_meta()->Clear();
                    _metadata->mutable_dcg_meta()->CopyFrom(copied_tablet_meta.dcg_meta());
                }

                if (copied_tablet_meta.has_sstable_meta()) {
                    _metadata->mutable_sstable_meta()->Clear();
                    _metadata->mutable_sstable_meta()->CopyFrom(copied_tablet_meta.sstable_meta());
                }

                if (copied_tablet_meta.has_delvec_meta()) {
                    _metadata->mutable_delvec_meta()->Clear();
                    _metadata->mutable_delvec_meta()->CopyFrom(copied_tablet_meta.delvec_meta());
                }

                _metadata->set_next_rowset_id(copied_tablet_meta.next_rowset_id());
                _metadata->set_cumulative_point(0);
                old_rowsets.Swap(_metadata->mutable_compaction_inputs());

                VLOG(3) << "Apply replication log with tablet metadata provided. tablet_id: " << _tablet.id()
                        << ", base_version: " << base_version << ", new_version: " << _new_version
                        << ", txn_id: " << txn_meta.txn_id() << ", metadata id: " << _metadata->id()
                        << ", next_rowset_id: " << _metadata->next_rowset_id()
                        << ", rowsets size: " << _metadata->rowsets_size();
            } else {
                auto old_rowsets = std::move(*_metadata->mutable_rowsets());
                _metadata->mutable_rowsets()->Clear();

                for (const auto& op_write : op_replication.op_writes()) {
                    RETURN_IF_ERROR(apply_write_log(op_write, txn_meta.txn_id()));
                }

                _metadata->set_cumulative_point(0);
                old_rowsets.Swap(_metadata->mutable_compaction_inputs());
            }

            LOG(INFO) << "Apply full replication log finish. tablet_id: " << _tablet.id()
                      << ", base_version: " << base_version << ", new_version: " << _new_version
                      << ", txn_id: " << txn_meta.txn_id();
        }

        if (op_replication.has_source_schema()) {
            _metadata->mutable_source_schema()->CopyFrom(op_replication.source_schema());
        }

        return Status::OK();
    }

    Tablet _tablet;
    MutableTabletMetadataPtr _metadata;
    int64_t _new_version;
    bool _skip_write_tablet_metadata;
};

std::unique_ptr<TxnLogApplier> new_txn_log_applier(const Tablet& tablet, MutableTabletMetadataPtr metadata,
                                                   int64_t new_version, bool rebuild_pindex,
                                                   bool skip_write_tablet_metadata) {
    if (metadata->schema().keys_type() == PRIMARY_KEYS) {
        return std::make_unique<PrimaryKeyTxnLogApplier>(tablet, std::move(metadata), new_version, rebuild_pindex,
                                                         skip_write_tablet_metadata);
    }
    return std::make_unique<NonPrimaryKeyTxnLogApplier>(tablet, std::move(metadata), new_version,
                                                        skip_write_tablet_metadata);
}

} // namespace starrocks::lake
