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
#include "exec/olap_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "runtime/exec_env.h"
#include "runtime/global_dict/fragment_dict_state.h"
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
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks {

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
};

Status OlapScanTabletAdaptor::init(int64_t tablet_id) {
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    ASSIGN_OR_RETURN(_tablet, tablet_manager->get_tablet_by_id(tablet_id, false));
    return Status::OK();
}

Status OlapScanTabletAdaptor::init_schema(RuntimeState* state) {
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
    const auto& olap_scan_node_desc = _glm_ctx->scan_node();
    std::vector<ColumnAccessPathPtr> access_paths;
    if (olap_scan_node_desc.__isset.column_access_paths) {
        for (int i = 0; i < olap_scan_node_desc.column_access_paths.size(); ++i) {
            auto st = ColumnAccessPath::create(olap_scan_node_desc.column_access_paths[i], state, pool);
            if (LIKELY(st.ok())) {
                access_paths.emplace_back(std::move(st.value()));
            } else {
                LOG(WARNING) << "Failed to create column access path: "
                             << olap_scan_node_desc.column_access_paths[i].type << "index: " << i
                             << ", error: " << st.status();
            }
        }
    }
    size_t next_uniq_id = olap_scan_node_desc.next_uniq_id;
    _column_access_paths = std::move(access_paths);
    ASSIGN_OR_RETURN(_tablet_schema, extend_schema_by_access_paths(_tablet_schema, next_uniq_id, _column_access_paths));
    return Status::OK();
}

Status OlapScanTabletAdaptor::init_global_dicts(RuntimeState* state, ObjectPool* pool,
                                                const std::vector<SlotDescriptor*>& slots) {
    const auto* fragment_dict_state = state->fragment_dict_state();
    DCHECK(fragment_dict_state != nullptr);
    const auto& global_dict_map = fragment_dict_state->query_global_dicts();

    auto global_dict = pool->add(new ColumnIdToGlobalDictMap());
    for (auto* slot : slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    _global_dicts = global_dict;

    return Status::OK();
}

Status OlapScanTabletAdaptor::init_read_columns(const std::vector<SlotDescriptor*>& slots) {
    std::vector<uint32_t> scanner_columns;
    for (const auto* slot : slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        scanner_columns.push_back(static_cast<uint32_t>(index));
    }
    std::sort(scanner_columns.begin(), scanner_columns.end());

    _read_schema = ChunkHelper::convert_schema(_tablet_schema, scanner_columns);

    return Status::OK();
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
    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
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
                                                 state->fragment_ctx()->fe_addr(), _tablet.metadata()));
    } else {
        _tablet_schema = _tablet.get_schema();
    }

    if (_tablet_schema == nullptr) {
        return Status::InternalError("lake tablet schema is nullptr");
    }
    return Status::OK();
}

Status LakeScanTabletAdaptor::init_access_path(RuntimeState* state, ObjectPool* pool) {
    const auto& lake_scan_desc = _glm_ctx->scan_node();
    std::vector<ColumnAccessPathPtr> access_paths;
    if (lake_scan_desc.__isset.column_access_paths) {
        for (int i = 0; i < lake_scan_desc.column_access_paths.size(); ++i) {
            auto st = ColumnAccessPath::create(lake_scan_desc.column_access_paths[i], state, pool);
            if (LIKELY(st.ok())) {
                access_paths.emplace_back(std::move(st.value()));
            } else {
                LOG(WARNING) << "Failed to create column access path: " << lake_scan_desc.column_access_paths[i].type
                             << "index: " << i << ", error: " << st.status();
            }
        }
    }
    _column_access_paths = std::move(access_paths);
    size_t next_uniq_id = lake_scan_desc.next_uniq_id;
    ASSIGN_OR_RETURN(_tablet_schema, extend_schema_by_access_paths(_tablet_schema, next_uniq_id, _column_access_paths));
    return Status::OK();
}

Status LakeScanTabletAdaptor::init_global_dicts(RuntimeState* state, ObjectPool* pool,
                                                const std::vector<SlotDescriptor*>& slots) {
    const auto* fragment_dict_state = state->fragment_dict_state();
    DCHECK(fragment_dict_state != nullptr);
    const auto& global_dict_map = fragment_dict_state->query_global_dicts();

    auto global_dict = pool->add(new ColumnIdToGlobalDictMap());
    for (auto* slot : slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    _global_dicts = global_dict;

    return Status::OK();
}

Status LakeScanTabletAdaptor::init_read_columns(const std::vector<SlotDescriptor*>& slots) {
    std::vector<uint32_t> scanner_columns;
    for (const auto* slot : slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        scanner_columns.push_back(static_cast<uint32_t>(index));
    }
    std::sort(scanner_columns.begin(), scanner_columns.end());

    _read_schema = ChunkHelper::convert_schema(_tablet_schema, scanner_columns);

    return Status::OK();
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
        auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
        _rowsets = lake::Rowset::get_rowsets(tablet_mgr, _tablet.metadata());
        target = _glm_ctx->get_rowset(_rowsets, static_cast<int32_t>(lake_rssid), &target_segment_idx);
    }

    if (target == nullptr) {
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
    rs_opts.stats = &_stats;
    rs_opts.global_dictmaps = _global_dicts;
    rs_opts.column_access_paths = &_column_access_paths;
    rs_opts.tablet_schema = _tablet_schema;
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        rs_opts.is_primary_keys = true;
        rs_opts.version = _glm_ctx->get_rowsets_version(_tablet_id);
    }

    const auto* segment = segments[target_segment_idx].get();
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