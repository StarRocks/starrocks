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

#include "exec/pipeline/lookup/tablet_adaptor.h"

#include <algorithm>
#include <limits>

#include "base/status.h"
#include "base/status_fmt.hpp"
#include "base/statusor.h"
#include "base/uid_util.h"
#include "common/config_lake_fwd.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "exec/exec_env.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "exprs/column_access_path_resolver.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/extends_column_utils.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/metadata_util.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/storage_engine.h"
#include "storage/storage_env.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks {

namespace detail {

StatusOr<std::vector<ColumnAccessPathPtr>> init_access_paths(const std::vector<TColumnAccessPath>& column_access_paths,
                                                             RuntimeState* state, ObjectPool* pool) {
    std::vector<ColumnAccessPathPtr> access_paths;
    auto path_resolver = make_column_access_path_resolver(state, pool);
    for (int i = 0; i < column_access_paths.size(); ++i) {
        auto st = ColumnAccessPath::create(column_access_paths[i], path_resolver);
        if (LIKELY(st.ok())) {
            access_paths.emplace_back(std::move(st.value()));
        } else {
            LOG(WARNING) << "Failed to create column access path: " << column_access_paths[i].type << "index: " << i
                         << ", error: " << st.status();
        }
    }
    return access_paths;
}

Status init_global_dicts_for_scan_node(RuntimeState* state, ObjectPool* pool, const TabletSchemaCSPtr& tablet_schema,
                                       const std::vector<SlotDescriptor*>& slots,
                                       ColumnIdToGlobalDictMap** global_dicts) {
    const auto* fragment_dict_state = state->fragment_dict_state();
    DCHECK(fragment_dict_state != nullptr);
    const auto& global_dict_map = fragment_dict_state->query_global_dicts();

    auto dicts = pool->add(new ColumnIdToGlobalDictMap());
    for (auto* slot : slots) {
        int32_t index = tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            dicts->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    *global_dicts = dicts;
    return Status::OK();
}

Status init_read_schema(const TabletSchemaCSPtr& tablet_schema, const std::vector<SlotDescriptor*>& slots,
                        starrocks::Schema* read_schema) {
    std::vector<uint32_t> scanner_columns;
    for (const auto* slot : slots) {
        int32_t index = tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        scanner_columns.push_back(static_cast<uint32_t>(index));
    }
    std::sort(scanner_columns.begin(), scanner_columns.end());
    *read_schema = ChunkHelper::convert_schema(tablet_schema, scanner_columns);
    return Status::OK();
}

} // namespace detail

class OlapScanTabletAdaptor final : public LookUpTabletAdaptor {
public:
    OlapScanTabletAdaptor() = default;
    ~OlapScanTabletAdaptor() override = default;

    Status capture(GlobalLateMaterilizationContext* glm_ctx) override {
        _glm_ctx = (OlapScanLazyMaterializationContext*)glm_ctx;
        return Status::OK();
    }

    Status init(int64_t tablet_id) override;

    Status init_schema(RuntimeState* state) override;

    Status init_access_path(RuntimeState* state, ObjectPool* pool) override;

    Status init_global_dicts(RuntimeState* state, ObjectPool* pool, const std::vector<SlotDescriptor*>& slots) override;

    Status init_read_columns(const std::vector<SlotDescriptor*>& slots) override;

    StatusOr<ChunkIteratorPtr> get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range) override;

private:
    TabletSharedPtr _tablet;
    TabletSchemaCSPtr _tablet_schema;
    starrocks::Schema _read_schema;
    std::vector<RowsetSharedPtr> _captured_rowsets;
    OlapReaderStatistics _stats;
    std::vector<ColumnAccessPathPtr> _column_access_paths;
    ColumnIdToGlobalDictMap* _global_dicts = nullptr;
    OlapScanLazyMaterializationContext* _glm_ctx = nullptr;
    bool _use_page_cache = false;
};

class LakeScanTabletAdaptor final : public LookUpTabletAdaptor {
public:
    LakeScanTabletAdaptor() = default;
    ~LakeScanTabletAdaptor() override = default;

    Status capture(GlobalLateMaterilizationContext* glm_ctx) override {
        _glm_ctx = (LakeScanLazyMaterializationContext*)glm_ctx;
        return Status::OK();
    }

    Status init(int64_t tablet_id) override;

    Status init_schema(RuntimeState* state) override;

    Status init_access_path(RuntimeState* state, ObjectPool* pool) override;

    Status init_global_dicts(RuntimeState* state, ObjectPool* pool, const std::vector<SlotDescriptor*>& slots) override;

    Status init_read_columns(const std::vector<SlotDescriptor*>& slots) override;

    StatusOr<ChunkIteratorPtr> get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range) override;

private:
    lake::VersionedTablet _tablet;
    int64_t _tablet_id = 0;
    TabletSchemaCSPtr _tablet_schema;
    starrocks::Schema _read_schema;
    OlapReaderStatistics _stats;
    std::vector<ColumnAccessPathPtr> _column_access_paths;
    ColumnIdToGlobalDictMap* _global_dicts = nullptr;
    LakeScanLazyMaterializationContext* _glm_ctx = nullptr;
    std::vector<lake::RowsetPtr> _rowsets;
    bool _use_page_cache = false;
    // [GLM-DIAG] correlation key with the [GLM-DIAG POS] scan-side line.
    std::string _query_id_str;
};

Status OlapScanTabletAdaptor::init(int64_t tablet_id) {
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    ASSIGN_OR_RETURN(_tablet, tablet_manager->get_tablet_by_id(tablet_id, false));
    return Status::OK();
}

Status OlapScanTabletAdaptor::init_schema(RuntimeState* state) {
    _use_page_cache = state->use_page_cache();
    const auto& thrift_olap_scan_node = _glm_ctx->scan_node();

    if (thrift_olap_scan_node.__isset.schema_id && thrift_olap_scan_node.schema_id > 0 &&
        thrift_olap_scan_node.schema_id == _tablet->tablet_schema()->id()) {
        _tablet_schema = _tablet->tablet_schema();
    } else {
        // if column_desc come from fe, reset tablet schema
        if (thrift_olap_scan_node.__isset.columns_desc && !thrift_olap_scan_node.columns_desc.empty() &&
            thrift_olap_scan_node.columns_desc[0].col_unique_id >= 0) {
            auto columns_desc_copy = thrift_olap_scan_node.columns_desc;
            Status preprocess_status = preprocess_default_expr_for_tcolumns(columns_desc_copy);
            WARN_IF_ERROR(preprocess_status, "Failed to preprocess default_expr in olap_scan_tablet_adaptor");
            _tablet_schema = TabletSchema::copy(*_tablet->tablet_schema(), columns_desc_copy);
        } else {
            _tablet_schema = _tablet->tablet_schema();
        }
    }

    return Status::OK();
}

Status OlapScanTabletAdaptor::init_access_path(RuntimeState* state, ObjectPool* pool) {
    using namespace detail;
    const auto& scan_node = _glm_ctx->scan_node();
    size_t next_unique_id = scan_node.next_uniq_id;
    ASSIGN_OR_RETURN(_column_access_paths, init_access_paths(scan_node.column_access_paths, state, pool));
    ASSIGN_OR_RETURN(_tablet_schema,
                     extend_schema_by_access_paths(_tablet_schema, next_unique_id, _column_access_paths));
    return Status::OK();
}

Status OlapScanTabletAdaptor::init_global_dicts(RuntimeState* state, ObjectPool* pool,
                                                const std::vector<SlotDescriptor*>& slots) {
    using namespace detail;
    return init_global_dicts_for_scan_node(state, pool, _tablet_schema, slots, &_global_dicts);
}

Status OlapScanTabletAdaptor::init_read_columns(const std::vector<SlotDescriptor*>& slots) {
    using namespace detail;
    return init_read_schema(_tablet_schema, slots, &_read_schema);
}

auto OlapScanTabletAdaptor::get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range)
        -> StatusOr<ChunkIteratorPtr> {
    RowsetSharedPtr target;
    int32_t segment_id;

    target = _glm_ctx->get_rowset(_tablet->tablet_id(), rssid, &segment_id);

    if (target == nullptr) {
        return Status::InternalError(fmt::format("not found rssid:{}", rssid));
    }

    auto rowid_range = std::make_shared<SparseRange<>>();
    *rowid_range = std::move(row_id_range);
    RowsetReadOptions rs_opts;
    rs_opts.rowid_range_option = std::make_shared<RowidRangeOption>();
    rs_opts.profile = nullptr;
    // Honor the same page-cache policy as the normal scan (RuntimeState::use_page_cache(),
    // which already respects the disable_storage_page_cache config and the session var).
    // Otherwise the lookup bypasses StoragePageCache and re-decompresses every fetched
    // column page per row locator. See olap_chunk_source.cpp.
    rs_opts.use_page_cache = _use_page_cache;
    rs_opts.stats = &_stats;
    rs_opts.global_dictmaps = _global_dicts;
    rs_opts.column_access_paths = &_column_access_paths;
    rs_opts.tablet_schema = _tablet_schema;
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _glm_ctx->get_rowsets_version(_tablet->tablet_id());
    }

    const auto* segment = target->segments()[segment_id].get();
    rs_opts.rowid_range_option->add(target.get(), segment, rowid_range, true);

    std::vector<ChunkIteratorPtr> iters;
    RETURN_IF_ERROR(target->get_segment_iterators(_read_schema, rs_opts, &iters));

    DCHECK_EQ(1, iters.size());
    if (iters.size() != 1) {
        return Status::InternalError("unexpected iterators from rowset");
    }

    return iters[0];
}

Status LakeScanTabletAdaptor::init(int64_t tablet_id) {
    _tablet_id = tablet_id;
    return Status::OK();
}

Status LakeScanTabletAdaptor::init_schema(RuntimeState* state) {
    _use_page_cache = state->use_page_cache();
    _query_id_str = print_id(state->query_id());
    auto* tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
    const auto& lake_scan_node = _glm_ctx->scan_node();

    auto version = _glm_ctx->get_rowsets_version(_tablet_id);
    ASSIGN_OR_RETURN(_tablet, tablet_mgr->get_tablet(_tablet_id, version));

    if (lake_scan_node.__isset.schema_key) {
        const auto& t_schema_key = lake_scan_node.schema_key;
        TableSchemaKeyPB schema_key_pb;
        schema_key_pb.set_schema_id(t_schema_key.schema_id);
        schema_key_pb.set_db_id(t_schema_key.db_id);
        schema_key_pb.set_table_id(t_schema_key.table_id);
        ASSIGN_OR_RETURN(_tablet_schema, tablet_mgr->table_schema_service()->get_schema_for_scan(
                                                 schema_key_pb, _tablet_id, state->query_id(),
                                                 state->fragment_runtime_state()->fe_addr(), _tablet.metadata()));
    } else {
        _tablet_schema = _tablet.get_schema();
    }

    if (_tablet_schema == nullptr) {
        return Status::InternalError("lake tablet schema is nullptr");
    }
    return Status::OK();
}

Status LakeScanTabletAdaptor::init_access_path(RuntimeState* state, ObjectPool* pool) {
    using namespace detail;

    const auto& scan_node = _glm_ctx->scan_node();
    size_t next_unique_id = scan_node.next_uniq_id;
    ASSIGN_OR_RETURN(_column_access_paths, init_access_paths(scan_node.column_access_paths, state, pool));
    ASSIGN_OR_RETURN(_tablet_schema,
                     extend_schema_by_access_paths(_tablet_schema, next_unique_id, _column_access_paths));
    return Status::OK();
}

Status LakeScanTabletAdaptor::init_global_dicts(RuntimeState* state, ObjectPool* pool,
                                                const std::vector<SlotDescriptor*>& slots) {
    using namespace detail;
    return init_global_dicts_for_scan_node(state, pool, _tablet_schema, slots, &_global_dicts);
}

Status LakeScanTabletAdaptor::init_read_columns(const std::vector<SlotDescriptor*>& slots) {
    using namespace detail;
    return init_read_schema(_tablet_schema, slots, &_read_schema);
}

auto LakeScanTabletAdaptor::get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range)
        -> StatusOr<ChunkIteratorPtr> {
    if (rssid < 0 || rssid > std::numeric_limits<uint32_t>::max()) {
        return Status::InternalError(fmt::format("invalid lake rssid:{}", rssid));
    }

    lake::RowsetPtr target;
    int32_t target_segment_idx = -1;
    uint32_t lake_rssid = static_cast<uint32_t>(rssid);

    target = _glm_ctx->get_rowset(static_cast<int32_t>(_tablet_id), static_cast<int32_t>(lake_rssid),
                                  &target_segment_idx);

    if (target == nullptr) {
        auto* tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
        _rowsets = lake::Rowset::get_rowsets(tablet_mgr, _tablet.metadata());
        target = _glm_ctx->get_rowset(_rowsets, static_cast<int32_t>(lake_rssid), &target_segment_idx);
    }

    if (target == nullptr) {
        // [GLM-DIAG] dump captured vs live rowset ranges to expose the scan-vs-fetch snapshot
        // mismatch behind "not found lake rssid". All PB access lives in debug_dump_ranges.
        LOG(WARNING) << "[GLM-DIAG notfound] query_id=" << _query_id_str << " tablet=" << _tablet_id
                     << " rssid=" << rssid << " " << _glm_ctx->debug_dump_ranges(static_cast<int32_t>(_tablet_id))
                     << " live_" << _glm_ctx->debug_dump_ranges(_rowsets);
        // [GLM-DIAG sstprov] Dump the tablet's PK-index SST filesets with split provenance, to pin
        // whether the failing rssid was served by a shared=1 (inherited-from-split-parent) SST vs a
        // shared=0 (locally-produced compaction) SST. A shared SST with shared_version>0 rewrites
        // every key it serves to the single value (shared_rssid + rssid_offset) — see
        // PersistentIndexSstable::multi_get. So `shared_hit_idx>=0` means the failing rssid was
        // produced by that inherited SST => (S1) split-time handoff; a shared=0 SST that should cover
        // the key yet lost to the inherited one => (S2) post-split compaction gap. See goal.
        {
            const auto& meta = _tablet.metadata();
            std::string dump;
            int shared_hit_idx = -1;
            int n_shared = 0, n_local = 0, n_sst = 0;
            int64_t meta_version = (meta != nullptr) ? meta->version() : -1;
            if (meta != nullptr && meta->has_sstable_meta()) {
                const auto& ssts = meta->sstable_meta().sstables();
                n_sst = ssts.size();
                for (int i = 0; i < ssts.size(); ++i) {
                    const auto& s = ssts.Get(i);
                    const bool is_shared = s.shared();
                    if (is_shared) {
                        ++n_shared;
                    } else {
                        ++n_local;
                    }
                    if (is_shared && s.has_shared_version() && s.shared_version() > 0) {
                        const int64_t eff = static_cast<int64_t>(s.shared_rssid()) + s.rssid_offset();
                        if (eff == rssid) {
                            shared_hit_idx = i;
                        }
                    }
                    dump += fmt::format(
                            " sst[{}]{{shared={} shared_rssid={} shared_version={} rssid_offset={} "
                            "has_range={} file={}}}",
                            i, is_shared ? 1 : 0, s.shared_rssid(), s.shared_version(), s.rssid_offset(),
                            s.has_range() ? 1 : 0, s.filename());
                }
            }
            LOG(WARNING) << "[GLM-DIAG sstprov] query_id=" << _query_id_str << " tablet=" << _tablet_id
                         << " rssid=" << rssid << " meta_version=" << meta_version << " n_sst=" << n_sst
                         << " n_shared=" << n_shared << " n_local=" << n_local << " shared_hit_idx=" << shared_hit_idx
                         << dump;
        }
        return Status::InternalError(fmt::format("not found lake rssid:{} in tablet_id:{}", rssid, _tablet_id));
    }

    ASSIGN_OR_RETURN(auto segments, target->segments(true));
    if (target_segment_idx < 0 || target_segment_idx >= segments.size()) {
        return Status::InternalError(
                fmt::format("invalid segment index:{} for lake rssid:{}", target_segment_idx, rssid));
    }

    auto rowid_range = std::make_shared<SparseRange<>>();
    *rowid_range = std::move(row_id_range);

    RowsetReadOptions rs_opts;
    rs_opts.rowid_range_option = std::make_shared<RowidRangeOption>();
    rs_opts.profile = nullptr;
    // Honor the same page-cache policy as the normal scan (RuntimeState::use_page_cache(),
    // which already respects the disable_storage_page_cache config and the session var).
    // Otherwise the lookup bypasses StoragePageCache and re-decompresses every fetched
    // column page per row locator. See lake_connector.cpp. Data-cache (fill_data_cache)
    // is left at its default so the lookup does not override the scan's per-range policy.
    rs_opts.use_page_cache = _use_page_cache;
    rs_opts.stats = &_stats;
    rs_opts.global_dictmaps = _global_dicts;
    rs_opts.column_access_paths = &_column_access_paths;
    rs_opts.tablet_schema = _tablet_schema;
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _glm_ctx->get_rowsets_version(_tablet_id);
    }

    const auto* segment = segments[target_segment_idx].get();
    // A GLM point-lookup must return exactly the rows at the requested locators: the consumer accumulates
    // the fetched rows and materializes them by a permutation built over the full locator set. Yielding no
    // row would leave the accumulated chunk shorter than the permutation (silent wrong rows, or a
    // null-chunk deref when every locator is lost). So a null segment -- which, unlike a scan, this path
    // cannot tolerate -- must fail cleanly here whether or not experimental_lake_ignore_lost_segment is on.
    if (segment == nullptr) {
        return Status::InternalError(fmt::format(
                "null segment (index:{}) for lake rssid:{}{}", target_segment_idx, rssid,
                config::experimental_lake_ignore_lost_segment
                        ? " (a segment lost + tolerated by experimental_lake_ignore_lost_segment; a point lookup "
                          "cannot locate rows in it)"
                        : " (unexpected: experimental_lake_ignore_lost_segment is off)"));
    }
    rs_opts.rowid_range_option->add(target.get(), segment, rowid_range, true);

    ASSIGN_OR_RETURN(auto iters, target->read(_read_schema, rs_opts));
    DCHECK_EQ(1, iters.size());
    if (iters.size() != 1) {
        return Status::InternalError("unexpected iterators from lake rowset");
    }

    return iters[0];
}

StatusOr<LookUpTabletAdaptorPtr> create_look_up_tablet_adaptor(RowPositionDescriptor::Type type) {
    switch (type) {
    case RowPositionDescriptor::Type::OLAP_SCAN:
        return std::make_unique<OlapScanTabletAdaptor>();
    case RowPositionDescriptor::Type::LAKE_SCAN:
        return std::make_unique<LakeScanTabletAdaptor>();
    default:
        return Status::NotSupported("unsupported RowPositionDescriptor type");
    }
}
} // namespace starrocks
