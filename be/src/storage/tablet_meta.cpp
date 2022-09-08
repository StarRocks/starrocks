// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_meta.cpp

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

#include "storage/tablet_meta.h"

#include <boost/algorithm/string.hpp>
#include <memory>
#include <sstream>

#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "storage/olap_common.h"
#include "storage/protobuf_file.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_schema_map.h"
#include "storage/tablet_updates.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace starrocks {

enum class FieldTypeVersion {
    kV1, // beta rowset with config::storage_format_version == 1
    kV2, // beta rowset with config::storage_format_version == 2
};

// Old version StarRocks use `TColumnType` to save type info, convert it into `TTypeDesc`.
static void convert_to_new_version(TColumn* tcolumn) {
    if (!tcolumn->__isset.type_desc) {
        tcolumn->__set_index_len(tcolumn->column_type.index_len);

        TScalarType scalar_type;
        scalar_type.__set_type(tcolumn->column_type.type);
        scalar_type.__set_len(tcolumn->column_type.len);
        scalar_type.__set_precision(tcolumn->column_type.precision);
        scalar_type.__set_scale(tcolumn->column_type.scale);

        tcolumn->type_desc.types.resize(1);
        tcolumn->type_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        tcolumn->type_desc.types.back().__set_scalar_type(scalar_type);
        tcolumn->__isset.type_desc = true;
    }
}

static FieldAggregationMethod TAggregationType2FieldAggregationMethod(TAggregationType::type agg_type) {
    switch (agg_type) {
    case TAggregationType::NONE:
        return OLAP_FIELD_AGGREGATION_NONE;
    case TAggregationType::MAX:
        return OLAP_FIELD_AGGREGATION_MAX;
    case TAggregationType::MIN:
        return OLAP_FIELD_AGGREGATION_MIN;
    case TAggregationType::REPLACE:
        return OLAP_FIELD_AGGREGATION_REPLACE;
    case TAggregationType::REPLACE_IF_NOT_NULL:
        return OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL;
    case TAggregationType::BITMAP_UNION:
        return OLAP_FIELD_AGGREGATION_BITMAP_UNION;
    case TAggregationType::HLL_UNION:
        return OLAP_FIELD_AGGREGATION_HLL_UNION;
    case TAggregationType::SUM:
        return OLAP_FIELD_AGGREGATION_SUM;
    case TAggregationType::PERCENTILE_UNION:
        return OLAP_FIELD_AGGREGATION_PERCENTILE_UNION;
    }
    return OLAP_FIELD_AGGREGATION_NONE;
}

static FieldType TPrimitiveType2FieldType(TPrimitiveType::type primitive_type, FieldTypeVersion v) {
    switch (primitive_type) {
    case TPrimitiveType::INVALID_TYPE:
    case TPrimitiveType::NULL_TYPE:
    case TPrimitiveType::BINARY:
    case TPrimitiveType::TIME:
        return OLAP_FIELD_TYPE_UNKNOWN;
    case TPrimitiveType::BOOLEAN:
        return OLAP_FIELD_TYPE_BOOL;
    case TPrimitiveType::TINYINT:
        return OLAP_FIELD_TYPE_TINYINT;
    case TPrimitiveType::SMALLINT:
        return OLAP_FIELD_TYPE_SMALLINT;
    case TPrimitiveType::INT:
        return OLAP_FIELD_TYPE_INT;
    case TPrimitiveType::BIGINT:
        return OLAP_FIELD_TYPE_BIGINT;
    case TPrimitiveType::FLOAT:
        return OLAP_FIELD_TYPE_FLOAT;
    case TPrimitiveType::DOUBLE:
        return OLAP_FIELD_TYPE_DOUBLE;
    case TPrimitiveType::DATE:
        return v == FieldTypeVersion::kV1 ? OLAP_FIELD_TYPE_DATE : OLAP_FIELD_TYPE_DATE_V2;
    case TPrimitiveType::DATETIME:
        return v == FieldTypeVersion::kV1 ? OLAP_FIELD_TYPE_DATETIME : OLAP_FIELD_TYPE_TIMESTAMP;
    case TPrimitiveType::CHAR:
        return OLAP_FIELD_TYPE_CHAR;
    case TPrimitiveType::LARGEINT:
        return OLAP_FIELD_TYPE_LARGEINT;
    case TPrimitiveType::VARCHAR:
        return OLAP_FIELD_TYPE_VARCHAR;
    case TPrimitiveType::HLL:
        return OLAP_FIELD_TYPE_HLL;
    case TPrimitiveType::DECIMAL:
    case TPrimitiveType::DECIMALV2:
        return v == FieldTypeVersion::kV1 ? OLAP_FIELD_TYPE_DECIMAL : OLAP_FIELD_TYPE_DECIMAL_V2;
    case TPrimitiveType::DECIMAL32:
        return OLAP_FIELD_TYPE_DECIMAL32;
    case TPrimitiveType::DECIMAL64:
        return OLAP_FIELD_TYPE_DECIMAL64;
    case TPrimitiveType::DECIMAL128:
        return OLAP_FIELD_TYPE_DECIMAL128;
    case TPrimitiveType::OBJECT:
        return OLAP_FIELD_TYPE_OBJECT;
    case TPrimitiveType::PERCENTILE:
        return OLAP_FIELD_TYPE_PERCENTILE;
    case TPrimitiveType::JSON:
        return OLAP_FIELD_TYPE_JSON;
    }
    return OLAP_FIELD_TYPE_UNKNOWN;
}

static Status TColumn2ColumnPB(int32_t unique_id, const TColumn& t_column, FieldTypeVersion v, ColumnPB* column_pb,
                               size_t depth = 0) {
    const int32_t kFakeUniqueId = -1;

    const std::vector<TTypeNode>& types = t_column.type_desc.types;
    if (depth == types.size()) {
        return Status::InvalidArgument("type nodes must ended with scalar type");
    }

    // No names provided for child columns, assign them a fake name.
    auto c_name = depth == 0 ? t_column.column_name : strings::Substitute("_$0_$1", t_column.column_name, depth);

    // A child column cannot be a key column.
    auto is_key = depth == 0 && t_column.is_key;
    bool is_nullable = depth > 0 || t_column.is_allow_null;

    column_pb->set_unique_id(unique_id);
    column_pb->set_name(c_name);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(is_nullable);
    if (depth > 0 || is_key) {
        auto agg_method = OLAP_FIELD_AGGREGATION_NONE;
        column_pb->set_aggregation(TabletColumn::get_string_by_aggregation_type(agg_method));
    } else {
        auto agg_method = TAggregationType2FieldAggregationMethod(t_column.aggregation_type);
        column_pb->set_aggregation(TabletColumn::get_string_by_aggregation_type(agg_method));
    }

    const TTypeNode& curr_type_node = types[depth];
    switch (curr_type_node.type) {
    case TTypeNodeType::SCALAR: {
        if (depth + 1 != types.size()) {
            return Status::InvalidArgument("scalar type cannot have child node");
        }
        TScalarType scalar = curr_type_node.scalar_type;

        FieldType field_type = TPrimitiveType2FieldType(scalar.type, v);
        column_pb->set_type(TabletColumn::get_string_by_field_type(field_type));
        column_pb->set_length(TabletColumn::get_field_length_by_type(field_type, scalar.len));
        column_pb->set_index_length(column_pb->length());
        column_pb->set_frac(curr_type_node.scalar_type.scale);
        column_pb->set_precision(curr_type_node.scalar_type.precision);
        if (field_type == OLAP_FIELD_TYPE_VARCHAR) {
            int32_t index_len = depth == 0 && t_column.__isset.index_len ? t_column.index_len : 10;
            column_pb->set_index_length(index_len);
        }
        if (depth == 0 && t_column.__isset.default_value) {
            column_pb->set_default_value(t_column.default_value);
        }
        if (depth == 0 && t_column.__isset.is_bloom_filter_column) {
            column_pb->set_is_bf_column(t_column.is_bloom_filter_column);
        }
        return Status::OK();
    }
    case TTypeNodeType::ARRAY:
        column_pb->set_type(TabletColumn::get_string_by_field_type(OLAP_FIELD_TYPE_ARRAY));
        column_pb->set_length(TabletColumn::get_field_length_by_type(OLAP_FIELD_TYPE_ARRAY, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());
        return TColumn2ColumnPB(kFakeUniqueId, t_column, v, column_pb->add_children_columns(), depth + 1);
    case TTypeNodeType::STRUCT:
        return Status::NotSupported("struct not supported yet");
    case TTypeNodeType::MAP:
        return Status::NotSupported("map not supported yet");
    }
    return Status::InternalError("Unreachable path");
}

void AlterTabletTask::init_from_pb(const AlterTabletPB& alter_task) {
    _alter_state = alter_task.alter_state();
    _related_tablet_id = alter_task.related_tablet_id();
    _related_schema_hash = alter_task.related_schema_hash();
    _alter_type = alter_task.alter_type();
}

void AlterTabletTask::to_alter_pb(AlterTabletPB* alter_task) {
    alter_task->set_alter_state(_alter_state);
    alter_task->set_related_tablet_id(_related_tablet_id);
    alter_task->set_related_schema_hash(_related_schema_hash);
    alter_task->set_alter_type(_alter_type);
}

Status AlterTabletTask::set_alter_state(AlterTabletState alter_state) {
    if (_alter_state == ALTER_FAILED && alter_state != ALTER_FAILED) {
        return Status::InvalidArgument("current state is ALTER_FAILED");
    } else if (_alter_state == ALTER_FINISHED && alter_state != ALTER_FINISHED) {
        return Status::InvalidArgument("current state is ALTER_FINISHED");
    }
    _alter_state = alter_state;
    return Status::OK();
}

Status TabletMeta::create(const TCreateTabletReq& request, const TabletUid& tablet_uid, uint64_t shard_id,
                          uint32_t next_unique_id,
                          const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                          RowsetTypePB rowset_type, TabletMetaSharedPtr* tablet_meta) {
    *tablet_meta = std::make_shared<TabletMeta>(
            request.table_id, request.partition_id, request.tablet_id, request.tablet_schema.schema_hash, shard_id,
            request.tablet_schema, next_unique_id,
            request.__isset.enable_persistent_index ? request.enable_persistent_index : false, col_ordinal_to_unique_id,
            tablet_uid, request.__isset.tablet_type ? request.tablet_type : TTabletType::TABLET_TYPE_DISK, rowset_type);
    return Status::OK();
}

TabletMetaSharedPtr TabletMeta::create() {
    return std::make_shared<TabletMeta>();
}

TabletMeta::TabletMeta(int64_t table_id, int64_t partition_id, int64_t tablet_id, int32_t schema_hash,
                       uint64_t shard_id, const TTabletSchema& tablet_schema, uint32_t next_unique_id,
                       bool enable_persistent_index,
                       const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                       const TabletUid& tablet_uid, TTabletType::type tabletType, RowsetTypePB rowset_type)
        : _tablet_uid(0, 0), _preferred_rowset_type(rowset_type) {
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_table_id(table_id);
    tablet_meta_pb.set_partition_id(partition_id);
    tablet_meta_pb.set_tablet_id(tablet_id);
    tablet_meta_pb.set_schema_hash(schema_hash);
    tablet_meta_pb.set_shard_id((int32_t)shard_id);
    tablet_meta_pb.set_creation_time(time(nullptr));
    tablet_meta_pb.set_cumulative_layer_point(-1);
    tablet_meta_pb.set_tablet_state(PB_RUNNING);
    tablet_meta_pb.set_enable_persistent_index(enable_persistent_index);
    *(tablet_meta_pb.mutable_tablet_uid()) = tablet_uid.to_proto();
    tablet_meta_pb.set_tablet_type(tabletType == TTabletType::TABLET_TYPE_MEMORY ? TabletTypePB::TABLET_TYPE_MEMORY
                                                                                 : TabletTypePB::TABLET_TYPE_DISK);
    TabletSchemaPB* schema = tablet_meta_pb.mutable_schema();
    if (tablet_schema.__isset.id) {
        schema->set_id(tablet_schema.id);
    }
    schema->set_num_short_key_columns(tablet_schema.short_key_column_count);
    schema->set_num_rows_per_row_block(config::default_num_rows_per_column_file_block);
    switch (tablet_schema.keys_type) {
    case TKeysType::DUP_KEYS:
        schema->set_keys_type(KeysType::DUP_KEYS);
        break;
    case TKeysType::UNIQUE_KEYS:
        schema->set_keys_type(KeysType::UNIQUE_KEYS);
        break;
    case TKeysType::AGG_KEYS:
        schema->set_keys_type(KeysType::AGG_KEYS);
        break;
    case TKeysType::PRIMARY_KEYS:
        schema->set_keys_type(KeysType::PRIMARY_KEYS);
        break;
    default:
        CHECK(false) << "unsupported keys type " << tablet_schema.keys_type;
    }
    schema->set_compress_kind(COMPRESS_LZ4);
    tablet_meta_pb.set_in_restore_mode(false);

    FieldTypeVersion field_version = FieldTypeVersion::kV1;
    if ((rowset_type == BETA_ROWSET) && (config::storage_format_version == 2)) {
        field_version = FieldTypeVersion::kV2;
    }

    // set column information
    uint32_t col_ordinal = 0;
    uint32_t key_count = 0;
    bool has_bf_columns = false;
    for (TColumn tcolumn : tablet_schema.columns) {
        convert_to_new_version(&tcolumn);
        uint32_t col_unique_id = col_ordinal_to_unique_id.at(col_ordinal++);
        ColumnPB* column = schema->add_column();

        TColumn2ColumnPB(col_unique_id, tcolumn, field_version, column);

        key_count += column->is_key();
        has_bf_columns |= column->is_bf_column();

        if (tablet_schema.__isset.indexes) {
            for (auto& index : tablet_schema.indexes) {
                if (index.index_type == TIndexType::type::BITMAP) {
                    DCHECK_EQ(index.columns.size(), 1);
                    if (boost::iequals(tcolumn.column_name, index.columns[0])) {
                        column->set_has_bitmap_index(true);
                        break;
                    }
                }
            }
        }
    }

    schema->set_next_column_unique_id(next_unique_id);
    if (has_bf_columns && tablet_schema.__isset.bloom_filter_fpp) {
        schema->set_bf_fpp(tablet_schema.bloom_filter_fpp);
    }

    // NOTE: The in-memory property is no longer supported
    // if (tablet_schema.__isset.is_in_memory) {
    //     schema->set_is_in_memory(tablet_schema.is_in_memory);
    // }

    init_from_pb(&tablet_meta_pb);
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->tablet_metadata_mem_tracker(), _mem_usage());
}

TabletMeta::TabletMeta() : _tablet_uid(0, 0) {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->tablet_metadata_mem_tracker(), _mem_usage());
}

TabletMeta::~TabletMeta() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->tablet_metadata_mem_tracker(), _mem_usage());
}

Status TabletMeta::create_from_file(const string& file_path) {
    TabletMetaPB tablet_meta_pb;
    ProtobufFile file(file_path);
    Status st = file.load(&tablet_meta_pb);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to load tablet meta file: " << st;
        return st;
    }
    init_from_pb(&tablet_meta_pb);
    return Status::OK();
}

Status TabletMeta::reset_tablet_uid(const string& file_path) {
    Status res;
    TabletMeta tmp_tablet_meta;
    if (res = tmp_tablet_meta.create_from_file(file_path); !res.ok()) {
        LOG(WARNING) << "fail to load tablet meta from " << file_path << ": " << res;
        return res;
    }
    TabletMetaPB tmp_tablet_meta_pb;
    tmp_tablet_meta.to_meta_pb(&tmp_tablet_meta_pb);
    *(tmp_tablet_meta_pb.mutable_tablet_uid()) = TabletUid::gen_uid().to_proto();
    if (res = save(file_path, tmp_tablet_meta_pb); !res.ok()) {
        LOG(FATAL) << "fail to save tablet meta pb to " << file_path << ": " << res;
        return res;
    }
    return res;
}

std::string TabletMeta::construct_header_file_path(const string& schema_hash_path, int64_t tablet_id) {
    std::stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    return header_name_stream.str();
}

Status TabletMeta::save(const string& file_path) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    return TabletMeta::save(file_path, tablet_meta_pb);
}

Status TabletMeta::save(const string& file_path, const TabletMetaPB& tablet_meta_pb) {
    DCHECK(!file_path.empty());
    ProtobufFile file(file_path);
    return file.save(tablet_meta_pb, true);
}

Status TabletMeta::save_meta(DataDir* data_dir) {
    std::unique_lock wrlock(_meta_lock);
    return _save_meta(data_dir);
}

Status TabletMeta::_save_meta(DataDir* data_dir) {
    LOG_IF(FATAL, _tablet_uid.hi == 0 && _tablet_uid.lo == 0)
            << "tablet_uid is invalid"
            << " tablet=" << full_name() << " _tablet_uid=" << _tablet_uid.to_string();
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    Status st = TabletMetaManager::save(data_dir, tablet_meta_pb);
    LOG_IF(FATAL, !st.ok()) << "fail to save tablet meta:" << st << ". tablet_id=" << tablet_id()
                            << ", schema_hash=" << schema_hash();
    return st;
}

Status TabletMeta::serialize(string* meta_binary) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    bool ok = tablet_meta_pb.SerializeToString(meta_binary);
    LOG_IF(FATAL, !ok) << "failed to serialize meta " << full_name();
    // deserialize the meta to check the result is correct
    TabletMetaPB de_tablet_meta_pb;
    ok = de_tablet_meta_pb.ParseFromString(*meta_binary);
    LOG_IF(FATAL, !ok) << "deserialize from previous serialize result failed " << full_name();
    return Status::OK();
}

Status TabletMeta::deserialize(std::string_view data) {
    TabletMetaPB tablet_meta_pb;
    if (!tablet_meta_pb.ParseFromArray(data.data(), data.size())) {
        LOG(WARNING) << "parse tablet meta failed";
        return Status::InternalError("parse TabletMetaPB from string failed");
    }
    init_from_pb(&tablet_meta_pb);
    return Status::OK();
}

void TabletMeta::init_from_pb(TabletMetaPB* ptablet_meta_pb) {
    auto& tablet_meta_pb = *ptablet_meta_pb;
    _table_id = tablet_meta_pb.table_id();
    _partition_id = tablet_meta_pb.partition_id();
    _tablet_id = tablet_meta_pb.tablet_id();
    _schema_hash = tablet_meta_pb.schema_hash();
    _shard_id = tablet_meta_pb.shard_id();
    _creation_time = tablet_meta_pb.creation_time();
    _cumulative_layer_point = tablet_meta_pb.cumulative_layer_point();
    _tablet_uid = TabletUid(tablet_meta_pb.tablet_uid());
    if (tablet_meta_pb.has_tablet_type()) {
        _tablet_type = tablet_meta_pb.tablet_type();
    } else {
        _tablet_type = TabletTypePB::TABLET_TYPE_DISK;
    }

    // _enable_persistent_index decide use persistent index in primary index or not
    // it is assigned when create tablet, and it can not be changed so far
    if (tablet_meta_pb.has_enable_persistent_index()) {
        _enable_persistent_index = tablet_meta_pb.enable_persistent_index();
    } else {
        _enable_persistent_index = false;
    }

    // init _tablet_state
    switch (tablet_meta_pb.tablet_state()) {
    case PB_NOTREADY:
        _tablet_state = TabletState::TABLET_NOTREADY;
        break;
    case PB_RUNNING:
        _tablet_state = TabletState::TABLET_RUNNING;
        break;
    case PB_TOMBSTONED:
        _tablet_state = TabletState::TABLET_TOMBSTONED;
        break;
    case PB_STOPPED:
        _tablet_state = TabletState::TABLET_STOPPED;
        break;
    case PB_SHUTDOWN:
        _tablet_state = TabletState::TABLET_SHUTDOWN;
        break;
    default:
        LOG(WARNING) << "tablet has no state. tablet=" << tablet_id() << ", schema_hash=" << schema_hash();
    }

    // init _schema
    if (tablet_meta_pb.schema().has_id() && tablet_meta_pb.schema().id() != TabletSchema::invalid_id()) {
        // Does not collect the memory usage of |_schema|.
        _schema = GlobalTabletSchemaMap::Instance()->emplace(tablet_meta_pb.schema()).first;
    } else {
        _schema = std::make_shared<const TabletSchema>(tablet_meta_pb.schema());
    }

    // init _rs_metas
    for (auto& it : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new RowsetMeta());
        rs_meta->init_from_pb(it);
        if (rs_meta->has_delete_predicate()) {
            add_delete_predicate(rs_meta->delete_predicate(), rs_meta->version().first);
        }
        _rs_metas.push_back(std::move(rs_meta));
    }
    for (auto& it : tablet_meta_pb.inc_rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new RowsetMeta());
        rs_meta->init_from_pb(it);
        _inc_rs_metas.push_back(std::move(rs_meta));
    }

    // generate AlterTabletTask
    if (tablet_meta_pb.has_alter_task()) {
        _alter_task = std::make_shared<AlterTabletTask>();
        _alter_task->init_from_pb(tablet_meta_pb.alter_task());
    }

    if (tablet_meta_pb.has_in_restore_mode()) {
        _in_restore_mode = tablet_meta_pb.in_restore_mode();
    }

    if (tablet_meta_pb.has_preferred_rowset_type()) {
        _preferred_rowset_type = tablet_meta_pb.preferred_rowset_type();
    }
    if (tablet_meta_pb.has_updates()) {
        _updatesPB.reset(tablet_meta_pb.release_updates());
    }
}

void TabletMeta::to_meta_pb(TabletMetaPB* tablet_meta_pb) {
    tablet_meta_pb->set_table_id(table_id());
    tablet_meta_pb->set_partition_id(partition_id());
    tablet_meta_pb->set_tablet_id(tablet_id());
    tablet_meta_pb->set_schema_hash(schema_hash());
    tablet_meta_pb->set_shard_id(shard_id());
    tablet_meta_pb->set_creation_time(creation_time());
    tablet_meta_pb->set_cumulative_layer_point(cumulative_layer_point());
    tablet_meta_pb->set_enable_persistent_index(get_enable_persistent_index());
    *tablet_meta_pb->mutable_tablet_uid() = tablet_uid().to_proto();
    tablet_meta_pb->set_tablet_type(_tablet_type);
    switch (tablet_state()) {
    case TABLET_NOTREADY:
        tablet_meta_pb->set_tablet_state(PB_NOTREADY);
        break;
    case TABLET_RUNNING:
        tablet_meta_pb->set_tablet_state(PB_RUNNING);
        break;
    case TABLET_TOMBSTONED:
        tablet_meta_pb->set_tablet_state(PB_TOMBSTONED);
        break;
    case TABLET_STOPPED:
        tablet_meta_pb->set_tablet_state(PB_STOPPED);
        break;
    case TABLET_SHUTDOWN:
        tablet_meta_pb->set_tablet_state(PB_SHUTDOWN);
        break;
    }

    for (auto& rs : _rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_rs_metas());
    }
    for (const auto& rs : _inc_rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_inc_rs_metas());
    }
    if (_schema != nullptr) {
        _schema->to_schema_pb(tablet_meta_pb->mutable_schema());
    }
    if (_alter_task != nullptr) {
        _alter_task->to_alter_pb(tablet_meta_pb->mutable_alter_task());
    }

    tablet_meta_pb->set_in_restore_mode(in_restore_mode());

    // to avoid modify tablet meta to the greatest extend
    if (_preferred_rowset_type == BETA_ROWSET) {
        tablet_meta_pb->set_preferred_rowset_type(_preferred_rowset_type);
    }
    if (_updates != nullptr) {
        _updates->to_updates_pb(tablet_meta_pb->mutable_updates());
    } else if (_updatesPB) {
        tablet_meta_pb->mutable_updates()->CopyFrom(*_updatesPB);
    }
}

void TabletMeta::to_json(string* json_string, json2pb::Pb2JsonOptions& options) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, json_string, options);
}

Version TabletMeta::max_version() const {
    Version max_version = {-1, 0};
    for (auto& rs_meta : _rs_metas) {
        if (rs_meta->end_version() > max_version.second) {
            max_version = rs_meta->version();
        }
    }
    return max_version;
}

Status TabletMeta::add_rs_meta(const RowsetMetaSharedPtr& rs_meta) {
    // consistency is guarantee by tablet
    _rs_metas.push_back(rs_meta);
    if (rs_meta->has_delete_predicate()) {
        add_delete_predicate(rs_meta->delete_predicate(), rs_meta->version().first);
    }

    return Status::OK();
}

void TabletMeta::delete_rs_meta_by_version(const Version& version, std::vector<RowsetMetaSharedPtr>* deleted_rs_metas) {
    auto it = _rs_metas.begin();
    while (it != _rs_metas.end()) {
        if ((*it)->version() == version) {
            if (deleted_rs_metas != nullptr) {
                deleted_rs_metas->push_back(*it);
            }
            _rs_metas.erase(it);
            return;
        }
        ++it;
    }
}

void TabletMeta::modify_rs_metas(const std::vector<RowsetMetaSharedPtr>& to_add,
                                 const std::vector<RowsetMetaSharedPtr>& to_delete) {
    // Remove to_delete rowsets from _rs_metas
    for (const auto& rs_to_del : to_delete) {
        auto it = _rs_metas.begin();
        while (it != _rs_metas.end()) {
            if (rs_to_del->version() == (*it)->version()) {
                if ((*it)->has_delete_predicate()) {
                    remove_delete_predicate_by_version((*it)->version());
                }
                _rs_metas.erase(it);
                // there should be only one rowset match the version
                break;
            }
            ++it;
        }
    }
    // put to_delete rowsets in _stale_rs_metas.
    _stale_rs_metas.insert(_stale_rs_metas.end(), to_delete.begin(), to_delete.end());

    // put to_add rowsets in _rs_metas.
    _rs_metas.insert(_rs_metas.end(), to_add.begin(), to_add.end());
}

void TabletMeta::revise_rs_metas(std::vector<RowsetMetaSharedPtr> rs_metas) {
    std::unique_lock wrlock(_meta_lock);
    // delete alter task
    _alter_task.reset();

    _rs_metas = std::move(rs_metas);
}

void TabletMeta::revise_inc_rs_metas(std::vector<RowsetMetaSharedPtr> rs_metas) {
    std::unique_lock wrlock(_meta_lock);
    // delete alter task
    _alter_task.reset();

    _inc_rs_metas = std::move(rs_metas);
}

Status TabletMeta::add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta) {
    // consistency is guarantee by tablet
    _inc_rs_metas.push_back(rs_meta);
    return Status::OK();
}

void TabletMeta::delete_stale_rs_meta_by_version(const Version& version) {
    auto it = _stale_rs_metas.begin();
    while (it != _stale_rs_metas.end()) {
        if ((*it)->version() == version) {
            it = _stale_rs_metas.erase(it);
            // version wouldn't be duplicate
            break;
        } else {
            it++;
        }
    }
}

void TabletMeta::delete_inc_rs_meta_by_version(const Version& version) {
    auto it = _inc_rs_metas.begin();
    while (it != _inc_rs_metas.end()) {
        if ((*it)->version() == version) {
            _inc_rs_metas.erase(it);
            break;
        } else {
            it++;
        }
    }
}

RowsetMetaSharedPtr TabletMeta::acquire_inc_rs_meta_by_version(const Version& version) const {
    for (auto it : _inc_rs_metas) {
        if (it->version() == version) {
            return it;
        }
    }
    return nullptr;
}

void TabletMeta::add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version) {
    for (auto& del_pred : _del_pred_array) {
        if (del_pred.version() == version) {
            *del_pred.mutable_sub_predicates() = delete_predicate.sub_predicates();
            return;
        }
    }
    DeletePredicatePB* del_pred = _del_pred_array.Add();
    del_pred->set_version(version);
    *del_pred->mutable_sub_predicates() = delete_predicate.sub_predicates();
    *del_pred->mutable_in_predicates() = delete_predicate.in_predicates();
}

void TabletMeta::remove_delete_predicate_by_version(const Version& version) {
    DCHECK(version.first == version.second) << "version=" << version;
    for (int ordinal = 0; ordinal < _del_pred_array.size(); ++ordinal) {
        const DeletePredicatePB& temp = _del_pred_array.Get(ordinal);
        if (temp.version() == version.first) {
            // log delete condition
            string del_cond_str;
            for (const auto& it : temp.sub_predicates()) {
                del_cond_str += it + ";";
            }
            LOG(INFO) << "remove one del_pred. version=" << temp.version() << ", condition=" << del_cond_str;

            // remove delete condition from PB
            _del_pred_array.SwapElements(ordinal, _del_pred_array.size() - 1);
            _del_pred_array.RemoveLast();
        }
    }
}

const DelPredicateArray& TabletMeta::delete_predicates() const {
    return _del_pred_array;
}

bool TabletMeta::version_for_delete_predicate(const Version& version) {
    if (version.first != version.second) {
        return false;
    }

    for (auto& del_pred : _del_pred_array) {
        if (del_pred.version() == version.first) {
            return true;
        }
    }

    return false;
}

// return value not reference
// MVCC modification for alter task, upper application get a alter task mirror
AlterTabletTaskSharedPtr TabletMeta::alter_task() {
    std::shared_lock rlock(_meta_lock);
    return _alter_task;
}

void TabletMeta::add_alter_task(const AlterTabletTask& alter_task) {
    std::unique_lock wrlock(_meta_lock);
    _alter_task = std::make_shared<AlterTabletTask>(alter_task);
}

void TabletMeta::delete_alter_task() {
    std::unique_lock wrlock(_meta_lock);
    _alter_task.reset();
}

// if alter task is nullptr, return error?
Status TabletMeta::set_alter_state(AlterTabletState alter_state) {
    std::unique_lock wrlock(_meta_lock);
    if (_alter_task == nullptr) {
        // alter state should be set to ALTER_PREPARED when starting to
        // alter tablet. In this scenario, _alter_task is null pointer.
        LOG(WARNING) << "original alter task is null, could not set state";
        return Status::InternalError("original alter task is null");
    } else {
        auto alter_tablet_task = std::make_shared<AlterTabletTask>(*_alter_task);
        RETURN_IF_ERROR(alter_tablet_task->set_alter_state(alter_state));
        _alter_task = alter_tablet_task;
        return Status::OK();
    }
}

std::string TabletMeta::full_name() const {
    std::stringstream ss;
    ss << _tablet_id << "." << _schema_hash << "." << _tablet_uid.to_string();
    return ss.str();
}

Status TabletMeta::set_partition_id(int64_t partition_id) {
    if ((_partition_id > 0 && _partition_id != partition_id) || partition_id < 1) {
        LOG(FATAL) << "cur partition id=" << _partition_id << " new partition id=" << partition_id << " not equal";
    }
    _partition_id = partition_id;
    return Status::OK();
}

void TabletMeta::create_inital_updates_meta() {
    CHECK(!_updatesPB) << "_updates should be empty";
    _updatesPB = std::make_unique<TabletUpdatesPB>();
    auto edit_version_meta_pb = _updatesPB->add_versions();
    auto edit_version_pb = edit_version_meta_pb->mutable_version();
    edit_version_pb->set_major(1);
    edit_version_pb->set_minor(0);
    edit_version_meta_pb->set_creation_time(creation_time());
    _updatesPB->mutable_apply_version()->set_major(edit_version_pb->major());
    _updatesPB->mutable_apply_version()->set_minor(edit_version_pb->minor());
    _updatesPB->set_next_log_id(0);
    _updatesPB->set_next_rowset_id(0);
}

bool operator==(const AlterTabletTask& a, const AlterTabletTask& b) {
    if (a._alter_state != b._alter_state) return false;
    if (a._related_tablet_id != b._related_tablet_id) return false;
    if (a._related_schema_hash != b._related_schema_hash) return false;
    if (a._alter_type != b._alter_type) return false;
    return true;
}

bool operator!=(const AlterTabletTask& a, const AlterTabletTask& b) {
    return !(a == b);
}

bool operator==(const TabletMeta& a, const TabletMeta& b) {
    if (a._table_id != b._table_id) return false;
    if (a._partition_id != b._partition_id) return false;
    if (a._tablet_id != b._tablet_id) return false;
    if (a._schema_hash != b._schema_hash) return false;
    if (a._shard_id != b._shard_id) return false;
    if (a._creation_time != b._creation_time) return false;
    if (a._cumulative_layer_point != b._cumulative_layer_point) return false;
    if (a._tablet_uid != b._tablet_uid) return false;
    if (a._tablet_type != b._tablet_type) return false;
    if (a._tablet_state != b._tablet_state) return false;
    if (!((a._schema == nullptr && b._schema == nullptr) ||
          (a._schema != nullptr && b._schema != nullptr && *a._schema == *b._schema))) {
        return false;
    }
    if (a._rs_metas.size() != b._rs_metas.size()) return false;
    for (int i = 0; i < a._rs_metas.size(); ++i) {
        if (a._rs_metas[i] != b._rs_metas[i]) return false;
    }
    if (a._inc_rs_metas.size() != b._inc_rs_metas.size()) return false;
    for (int i = 0; i < a._inc_rs_metas.size(); ++i) {
        if (a._inc_rs_metas[i] != b._inc_rs_metas[i]) return false;
    }
    if (a._alter_task != b._alter_task) return false;
    if (a._in_restore_mode != b._in_restore_mode) return false;
    if (a._preferred_rowset_type != b._preferred_rowset_type) return false;
    return true;
}

bool operator!=(const TabletMeta& a, const TabletMeta& b) {
    return !(a == b);
}

} // namespace starrocks
