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

#include "gutil/strings/join.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "testutil/sync_point.h"
#include "util/dynamic_cache.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "util/trace.h"

namespace starrocks::lake {
class PrimaryKeyTxnLogApplier : public TxnLogApplier {
    template <class T>
    using ParallelSet =
            phmap::parallel_flat_hash_set<T, phmap::priv::hash_default_hash<T>, phmap::priv::hash_default_eq<T>,
                                          phmap::priv::Allocator<T>, 4, std::mutex, true>;

public:
    PrimaryKeyTxnLogApplier(Tablet tablet, MutableTabletMetadataPtr metadata, int64_t new_version)
            : _tablet(tablet),
              _metadata(std::move(metadata)),
              _base_version(_metadata->version()),
              _new_version(new_version),
              _builder(_tablet, _metadata) {
        _metadata->set_version(_new_version);
    }

    ~PrimaryKeyTxnLogApplier() override {
        // must release primary index before `handle_failure`, otherwise `handle_failure` will fail
        _tablet.update_mgr()->release_primary_index(_index_entry);
        // handle failure first, then release lock
        _builder.handle_failure();
        if (_inited) {
            _s_schema_change_set.erase(_tablet.id());
        }
    }

    Status init() override {
        auto [iter, ok] = _s_schema_change_set.insert(_tablet.id());
        if (ok) {
            _inited = true;
            return check_meta_version();
        } else {
            return Status::InternalError("primary key does not support concurrent log applying");
        }
    }

    Status check_meta_version() {
        // check tablet meta
        RETURN_IF_ERROR(_tablet.update_mgr()->check_meta_version(_tablet, _base_version));
        return Status::OK();
    }

    Status apply(const TxnLogPB& log) override {
        _max_txn_id = std::max(_max_txn_id, log.txn_id());
        if (log.has_op_write()) {
            RETURN_IF_ERROR(apply_write_log(log.op_write(), log.txn_id()));
        }
        if (log.has_op_compaction()) {
            RETURN_IF_ERROR(apply_compaction_log(log.op_compaction()));
        }
        if (log.has_op_schema_change()) {
            RETURN_IF_ERROR(apply_schema_change_log(log.op_schema_change()));
        }
        if (log.has_op_alter_metadata()) {
            RETURN_IF_ERROR(apply_alter_meta_log(log.op_alter_metadata()));
        }
        return Status::OK();
    }

    Status finish() override {
        // Must call `commit_primary_index` before `finalize`,
        // because if `commit_primary_index` or `finalize` fail, we can remove index in `handle_failure`.
        // if `_index_entry` is null, do nothing.
        RETURN_IF_ERROR(_tablet.update_mgr()->commit_primary_index(_index_entry, &_tablet));
        return _builder.finalize(_max_txn_id);
    }

    std::shared_ptr<std::vector<std::string>> trash_files() override { return _builder.trash_files(); }

private:
    Status apply_write_log(const TxnLogPB_OpWrite& op_write, int64_t txn_id) {
        if (op_write.dels_size() == 0 && op_write.rowset().num_rows() == 0 &&
            !op_write.rowset().has_delete_predicate()) {
            return Status::OK();
        }
        // We call `prepare_primary_index` only when first time we apply `write_log` or `compaction_log`, instead of
        // in `TxnLogApplier.init`, because we have to build primary index after apply `schema_change_log` finish.
        if (_index_entry == nullptr) {
            ASSIGN_OR_RETURN(_index_entry, _tablet.update_mgr()->prepare_primary_index(*_metadata, &_tablet, &_builder,
                                                                                       _base_version, _new_version));
        }
        return _tablet.update_mgr()->publish_primary_key_tablet(op_write, txn_id, *_metadata, &_tablet, _index_entry,
                                                                &_builder, _base_version);
    }

    Status apply_compaction_log(const TxnLogPB_OpCompaction& op_compaction) {
        if (op_compaction.input_rowsets().empty()) {
            DCHECK(!op_compaction.has_output_rowset() || op_compaction.output_rowset().num_rows() == 0);
            return Status::OK();
        }
        // We call `prepare_primary_index` only when first time we apply `write_log` or `compaction_log`, instead of
        // in `TxnLogApplier.init`, because we have to build primary index after apply `schema_change_log` finish.
        if (_index_entry == nullptr) {
            ASSIGN_OR_RETURN(_index_entry, _tablet.update_mgr()->prepare_primary_index(*_metadata, &_tablet, &_builder,
                                                                                       _base_version, _new_version));
        }
        return _tablet.update_mgr()->publish_primary_compaction(op_compaction, *_metadata, _tablet, _index_entry,
                                                                &_builder, _base_version);
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

    Status apply_alter_meta_log(const TxnLogPB_OpAlterMetadata& op_alter_metas) {
        DCHECK_EQ(_base_version + 1, _new_version);
        for (const auto& alter_meta : op_alter_metas.metadata_update_infos()) {
            if (alter_meta.has_enable_persistent_index()) {
                // this should always be true,
                // for FE will check whether the value of `enable_persisent_index` is changed or not
                // then send the alter task to BE
                if (_metadata->enable_persistent_index() != alter_meta.enable_persistent_index()) {
                    _metadata->set_enable_persistent_index(alter_meta.enable_persistent_index());

                    // Try remove index from index cache
                    // If tablet is doing apply rowset right now, remove primary index from index cache may be failed
                    // because the primary index is available in cache
                    // But it will be remove from index cache after apply is finished
                    (void)_tablet.update_mgr()->index_cache().try_remove_by_key(_tablet.id());
                } else {
                    LOG(WARNING) << strings::Substitute(
                            "alter_meta_log not need to apply, for enable_persistent_index is the same, which is $0, "
                            "base_version: $1, new_version: $2",
                            _metadata->enable_persistent_index(), _base_version, _new_version);
                }
            }
        }
        return Status::OK();
    }

    static inline ParallelSet<int64_t> _s_schema_change_set;

    Tablet _tablet;
    MutableTabletMetadataPtr _metadata;
    int64_t _base_version{0};
    int64_t _new_version{0};
    int64_t _max_txn_id{0}; // Used as the file name prefix of the delvec file
    MetaFileBuilder _builder;
    DynamicCache<uint64_t, LakePrimaryIndex>::Entry* _index_entry{nullptr};
    bool _inited{false};
};

class NonPrimaryKeyTxnLogApplier : public TxnLogApplier {
public:
    NonPrimaryKeyTxnLogApplier(Tablet tablet, MutableTabletMetadataPtr metadata, int64_t new_version)
            : _tablet(tablet), _metadata(std::move(metadata)), _new_version(new_version) {}

    Status apply(const TxnLogPB& log) override {
        if (log.has_op_write()) {
            RETURN_IF_ERROR(apply_write_log(log.op_write()));
        }
        if (log.has_op_compaction()) {
            RETURN_IF_ERROR(apply_compaction_log(log.op_compaction()));
        }
        if (log.has_op_schema_change()) {
            RETURN_IF_ERROR(apply_schema_change_log(log.op_schema_change()));
        }
        return Status::OK();
    }

    Status finish() override {
        _metadata->set_version(_new_version);
        return _tablet.put_metadata(_metadata);
    }

    std::shared_ptr<std::vector<std::string>> trash_files() override { return nullptr; }

private:
    Status apply_write_log(const TxnLogPB_OpWrite& op_write) {
        TEST_ERROR_POINT("NonPrimaryKeyTxnLogApplier::apply_write_log");
        if (op_write.has_rowset() && (op_write.rowset().num_rows() > 0 || op_write.rowset().has_delete_predicate())) {
            auto rowset = _metadata->add_rowsets();
            rowset->CopyFrom(op_write.rowset());
            rowset->set_id(_metadata->next_rowset_id());
            _metadata->set_next_rowset_id(_metadata->next_rowset_id() + std::max(1, rowset->segments_size()));
        }
        return Status::OK();
    }

    Status apply_compaction_log(const TxnLogPB_OpCompaction& op_compaction) {
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

        const auto end_input_pos = pre_input_pos + 1;

        for (auto iter = first_input_pos; iter != end_input_pos; ++iter) {
            _metadata->mutable_compaction_inputs()->Add(std::move(*iter));
        }

        auto first_idx = static_cast<uint32_t>(first_input_pos - _metadata->mutable_rowsets()->begin());
        if (op_compaction.has_output_rowset() && op_compaction.output_rowset().num_rows() > 0) {
            // Replace the first input rowset with output rowset
            auto output_rowset = _metadata->mutable_rowsets(first_idx);
            output_rowset->CopyFrom(op_compaction.output_rowset());
            output_rowset->set_id(_metadata->next_rowset_id());
            _metadata->set_next_rowset_id(_metadata->next_rowset_id() + output_rowset->segments_size());
            ++first_input_pos;
        }
        // Erase input rowsets from _metadata
        _metadata->mutable_rowsets()->erase(first_input_pos, end_input_pos);

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

    Tablet _tablet;
    MutableTabletMetadataPtr _metadata;
    int64_t _new_version;
};

std::unique_ptr<TxnLogApplier> new_txn_log_applier(Tablet tablet, MutableTabletMetadataPtr metadata,
                                                   int64_t new_version) {
    if (metadata->schema().keys_type() == PRIMARY_KEYS) {
        return std::make_unique<PrimaryKeyTxnLogApplier>(tablet, std::move(metadata), new_version);
    }
    return std::make_unique<NonPrimaryKeyTxnLogApplier>(tablet, std::move(metadata), new_version);
}

} // namespace starrocks::lake
