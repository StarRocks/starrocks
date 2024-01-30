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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_meta_manager.cpp

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

#include "storage/tablet_meta_manager.h"

#include <json2pb/json_to_pb.h>
#include <json2pb/pb_to_json.h>
#include <rapidjson/writer.h>
#include <rocksdb/write_batch.h>

#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "common/tracer.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/kv_store.h"
#include "storage/olap_define.h"
#include "storage/rocksdb_status_adapter.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_updates.h"
#include "util/coding.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/url_coding.h"

namespace starrocks {

static const char HEADER_PREFIX[] = "tabletmeta_";
static const std::string TABLET_META_LOG_PREFIX = "tlg_";
static const std::string TABLET_META_ROWSET_PREFIX = "trs_";
static const std::string TABLET_META_PENDING_ROWSET_PREFIX = "tpr_";
static const std::string TABLET_DELVEC_PREFIX = "dlv_";
static const std::string TABLET_PERSISTENT_INDEX_META_PREFIX = "tpi_";
static const std::string TABLET_DELTA_COLUMN_GROUP_PREFIX = "dcg_";

static string encode_meta_log_key(TTabletId id, uint64_t logid);
static bool decode_meta_log_key(std::string_view key, TTabletId* id, uint64_t* logid);
static string encode_meta_rowset_key(TTabletId id, uint32_t rowsetid);
[[maybe_unused]] static bool decode_meta_rowset_key(std::string_view key, TTabletId* id, uint32_t* rowsetid);
static string encode_meta_pending_rowset_key(TTabletId id, int64_t version);
[[maybe_unused]] static bool decode_meta_pending_rowset_key(std::string_view key, TTabletId* id, int64_t* version);
std::string encode_del_vector_key(TTabletId tablet_id, uint32_t segment_id, int64_t version);
void decode_del_vector_key(std::string_view enc_key, TTabletId* tablet_id, uint32_t* segment_id, int64_t* version);
std::string encode_persistent_index_key(TTabletId tablet_id);
void decode_persistent_index_key(std::string_view enc_key, TTabletId* tablet_id);
std::string encode_delta_column_group_key(TTabletId tablet_id, uint32_t segment_id, int64_t version);
std::string encode_delta_column_group_key(TTabletId tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version);
std::string encode_delta_column_group_key(TTabletId tablet_id, std::string rowsetid, uint32_t segment_id,
                                          int64_t version);
void decode_delta_column_group_key(std::string_view enc_key, TTabletId* tablet_id, uint32_t* segment_id,
                                   int64_t* version);

static std::string encode_tablet_meta_key(TTabletId tablet_id, TSchemaHash schema_hash) {
    return strings::Substitute("$0$1_$2", HEADER_PREFIX, tablet_id, schema_hash);
}

static bool decode_tablet_meta_key(std::string_view key, TTabletId* tablet_id, TSchemaHash* schema_hash) {
    if (UNLIKELY(key.size() <= sizeof(HEADER_PREFIX))) {
        return false;
    }
    DCHECK_EQ(0, key.compare(0, sizeof(HEADER_PREFIX) - 1, HEADER_PREFIX)) << key;
    key.remove_prefix(sizeof(HEADER_PREFIX) - 1);
    const char* str = key.data();
    const char* end = key.data() + key.size();
    const char* sep = static_cast<const char*>(memchr(str, '_', key.size()));
    if (UNLIKELY(sep == nullptr)) {
        return false;
    }
    if (!safe_strto64(str, sep - str, tablet_id)) {
        return false;
    }
    str = sep + 1;
    // |key| is not a zero terminated string, cannot use std::strtol here.
    return safe_strto32(str, end - str, schema_hash);
}

Status TabletMetaManager::get_primary_meta(KVStore* meta, TTabletId tablet_id, TabletMetaPB& tablet_meta_pb,
                                           string* json_meta) {
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    json2pb::ProtoMessageToJson(tablet_meta_pb, json_meta, json_options);

    Status st = Status::OK();
    string pbdata;
    string prefix;
    bool parsed = false;
    bool first = true;
    auto traverse_rst_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        uint32_t rowset_id;
        if (!decode_meta_rowset_key(key, &tid, &rowset_id)) {
            LOG(WARNING) << "invalid rowsetid key:" << key;
            return false;
        }
        if (tid == tablet_id) {
            RowsetMetaPB rowset_meta_pb;
            parsed = rowset_meta_pb.ParseFromArray(value.data(), value.length());
            if (!parsed) {
                st = Status::Corruption("bad rowset meta pb data");
                return false;
            }
            std::string rowset_meta;
            json2pb::ProtoMessageToJson(rowset_meta_pb, &rowset_meta, json_options);

            if (first) {
                json_meta->append(",\n\"applied_rs_metas\": [\n").append(rowset_meta);
            } else {
                json_meta->append(",\n").append(rowset_meta);
            }
            first = false;
            return true;
        } else {
            return false;
        }
    };
    first = true;
    prefix.clear();
    prefix.reserve(TABLET_META_ROWSET_PREFIX.length() + sizeof(uint64_t));
    prefix.append(TABLET_META_ROWSET_PREFIX);
    put_fixed64_le(&prefix, BigEndian::FromHost64(tablet_id));
    auto ret = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, traverse_rst_func);
    if (!ret.ok()) {
        return Status::InternalError("scan meta error");
    }
    if (!st.ok()) {
        return st;
    }
    if (!first) {
        json_meta->append("\n]");
    }

    auto traverse_pending_rst_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        int64_t version;
        if (!decode_meta_pending_rowset_key(key, &tid, &version)) {
            LOG(WARNING) << "invalid pending rowsetid key:" << key;
            return false;
        }
        if (tid == tablet_id) {
            RowsetMetaPB rowset_meta_pb;
            parsed = rowset_meta_pb.ParseFromArray(value.data(), value.length());
            if (!parsed) {
                st = Status::Corruption("bad pending rowset meta pb data");
                return false;
            }
            std::string rowset_json;
            json2pb::ProtoMessageToJson(rowset_meta_pb, &rowset_json, json_options);
            std::string pending_pre = "{\n\"version\": ";
            pending_pre.append(std::to_string(version)).append(",\n\"rs_meta\": ");

            rowset_json.insert(0, pending_pre);
            rowset_json.append("\n}");
            if (first) {
                json_meta->append(",\n\"pending_rs_metas\": [\n").append(rowset_json);
            } else {
                json_meta->append(",\n").append(rowset_json);
            }
            first = false;

            return true;
        }
        return false;
    };
    first = true;
    prefix.clear();
    prefix.reserve(TABLET_META_PENDING_ROWSET_PREFIX.length() + sizeof(uint64_t));
    prefix.append(TABLET_META_PENDING_ROWSET_PREFIX);
    put_fixed64_le(&prefix, BigEndian::FromHost64(tablet_id));
    ret = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, traverse_pending_rst_func);
    if (!ret.ok()) {
        return Status::InternalError("scan meta error");
    }
    if (!st.ok()) {
        return st;
    }
    if (!first) {
        json_meta->append("\n]");
    }

    auto traverse_log_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        uint64_t logid;
        if (!decode_meta_log_key(key, &tid, &logid)) {
            LOG(WARNING) << "invalid log meta key:" << key;
            return false;
        }
        if (tid == tablet_id) {
            TabletMetaLogPB tablet_meta_log_pb;
            parsed = tablet_meta_log_pb.ParseFromArray(value.data(), value.length());
            if (!parsed) {
                st = Status::Corruption("bad tablet log pb data");
                return false;
            }
            std::string log_json;
            json2pb::ProtoMessageToJson(tablet_meta_log_pb, &log_json, json_options);
            std::string log_pre = "{\n\"logid\": ";
            log_pre.append(std::to_string(logid)).append(",\n\"tablet_meta_log\": ");

            log_json.insert(0, log_pre);
            log_json.append("\n}");
            if (first) {
                json_meta->append(",\n\"tablet_meta_logs\": [\n").append(log_json);
            } else {
                json_meta->append(",\n").append(log_json);
            }
            first = false;

            return true;
        }
        return false;
    };
    first = true;
    prefix.clear();
    prefix.reserve(TABLET_META_LOG_PREFIX.length() + sizeof(uint64_t));
    prefix.append(TABLET_META_LOG_PREFIX);
    put_fixed64_le(&prefix, BigEndian::FromHost64(tablet_id));
    ret = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, traverse_log_func);
    if (!ret.ok()) {
        return Status::InternalError("scan meta error");
    }
    if (!st.ok()) {
        return st;
    }
    if (!first) {
        json_meta->append("\n]");
    }
    auto traverse_dlv_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        uint32_t segment_id;
        int64_t version;
        decode_del_vector_key(key, &tid, &segment_id, &version);

        if (tid == tablet_id) {
            if (value.length() == 0) {
                st = Status::Corruption("bad del vector data");
                return false;
            }
            std::string del_vector_json;
            if (first) {
                del_vector_json.append(",\n\"del_vectors\": [\n");
                first = false;
            } else {
                del_vector_json.append(",\n");
            }
            std::string encode_value;
            base64_encode(std::string(value.data(), value.length()), &encode_value);
            std::string version_str;
            version_str = "{\n    \"version\": ";
            version_str.append(std::to_string(version)).append(",");
            std::string segment_str;
            segment_str = "\n    \"segment_id\": ";
            segment_str.append(std::to_string(segment_id)).append(",");
            std::string val_str;
            val_str = "\n    \"base64_val\": \"";
            val_str.append(encode_value).append("\"\n}");

            del_vector_json.append(version_str).append(segment_str).append(val_str);
            json_meta->append(del_vector_json);
            return true;
        }
        return false;
    };
    first = true;
    prefix.clear();
    prefix.reserve(24);
    prefix.append(TABLET_DELVEC_PREFIX);
    put_fixed64_le(&prefix, BigEndian::FromHost64(tablet_id));
    ret = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, traverse_dlv_func);
    if (!ret.ok()) {
        return Status::InternalError("scan meta error");
    }
    if (!st.ok()) {
        return st;
    }
    if (!first) {
        json_meta->append("\n]");
    }

    json_meta->insert(0, "{\n\"tablet_meta\": ");
    json_meta->append("\n}");
    return Status::OK();
}

// should use tablet's generate tablet meta copy method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
Status TabletMetaManager::get_tablet_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                          TabletMeta* tablet_meta) {
    std::string key = encode_tablet_meta_key(tablet_id, schema_hash);
    std::string value;
    RETURN_IF_ERROR(store->get_meta()->get(META_COLUMN_FAMILY_INDEX, key, &value));
    return tablet_meta->deserialize(value);
}

Status TabletMetaManager::get_persistent_index_meta(DataDir* store, TTabletId tablet_id,
                                                    PersistentIndexMetaPB* index_meta) {
    std::string key = encode_persistent_index_key(tablet_id);
    std::string value;
    RETURN_IF_ERROR(store->get_meta()->get(META_COLUMN_FAMILY_INDEX, key, &value));
    index_meta->ParseFromString(value);
    return Status::OK();
}

Status TabletMetaManager::get_json_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                        std::string* json_meta) {
    TabletMeta tablet_meta;
    RETURN_IF_ERROR(get_tablet_meta(store, tablet_id, schema_hash, &tablet_meta));

    if (tablet_meta.tablet_schema_ptr()->keys_type() != PRIMARY_KEYS) {
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        tablet_meta.to_json(json_meta, json_options);
        return Status::OK();
    }

    TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    KVStore* meta = store->get_meta();
    return get_primary_meta(meta, tablet_id, tablet_meta_pb, json_meta);
}

Status TabletMetaManager::get_json_meta(DataDir* store, TTabletId tablet_id, std::string* json_meta) {
    string pbdata;
    KVStore* meta = store->get_meta();
    Status st;
    auto traverse_tabletmeta_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        TSchemaHash thash;
        if (!decode_tablet_meta_key(key, &tid, &thash)) {
            LOG(WARNING) << "invalid tablet_meta key:" << key;
            st = Status::Corruption("invalid tablet meta");
            return false;
        }
        if (tid == tablet_id) {
            // TODO(cbl): check multiple meta with same tablet_id exists
            pbdata = value;
            return false;
        } else {
            st = Status::NotFound("");
            return false;
        }
    };
    string prefix = strings::Substitute("$0$1_", HEADER_PREFIX, tablet_id);
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, traverse_tabletmeta_func));
    RETURN_IF_ERROR(st);
    TabletMetaPB tablet_meta_pb;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool parsed = tablet_meta_pb.ParseFromString(pbdata);
    if (tablet_meta_pb.schema().keys_type() != PRIMARY_KEYS) {
        if (!parsed) {
            return Status::Corruption("bad tablet meta pb data");
        }
        json2pb::ProtoMessageToJson(tablet_meta_pb, json_meta, json_options);
        return Status::OK();
    }
    return get_primary_meta(meta, tablet_id, tablet_meta_pb, json_meta);
}

Status TabletMetaManager::save(DataDir* store, const TabletMetaPB& meta_pb) {
    if (meta_pb.schema().keys_type() != KeysType::PRIMARY_KEYS && meta_pb.has_updates()) {
        return Status::InvalidArgument("non primary key with updates");
    }
    std::string key = encode_tablet_meta_key(meta_pb.tablet_id(), meta_pb.schema_hash());
    std::string val = meta_pb.SerializeAsString();

    TabletMetaPB tmp;
    if (!tmp.ParseFromString(val)) {
        LOG(FATAL) << "deserialize from previous serialize result failed";
    }

    rocksdb::WriteBatch batch;
    rocksdb::ColumnFamilyHandle* cf = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    rocksdb::Status st = batch.Put(cf, key, val);
    if (!st.ok()) {
        return to_status(st);
    }

    if (meta_pb.has_updates() && meta_pb.updates().has_next_log_id()) {
        std::string lower = encode_meta_log_key(meta_pb.tablet_id(), 0);
        std::string upper = encode_meta_log_key(meta_pb.tablet_id(), meta_pb.updates().next_log_id());
        st = batch.DeleteRange(cf, lower, upper);
        if (!st.ok()) {
            return to_status(st);
        }
    }
    return store->get_meta()->write_batch(&batch);
}

Status TabletMetaManager::remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash) {
    WriteBatch wb;
    RETURN_IF_ERROR(remove_tablet_meta(store, &wb, tablet_id, schema_hash));
    return store->get_meta()->write_batch(&wb);
}

Status TabletMetaManager::walk(
        KVStore* meta,
        std::function<bool(long /*tablet_id*/, long /*schema_hash*/, std::string_view /*meta*/)> const& func) {
    return walk_until_timeout(meta, func, -1);
}

Status TabletMetaManager::walk_until_timeout(
        KVStore* meta,
        std::function<bool(long /*tablet_id*/, long /*schema_hash*/, std::string_view /*meta*/)> const& func,
        int64_t timeout_sec) {
    auto traverse_header_func = [&func](std::string_view key, std::string_view value) -> bool {
        TTabletId tablet_id;
        TSchemaHash schema_hash;
        if (!decode_tablet_meta_key(key, &tablet_id, &schema_hash)) {
            LOG(WARNING) << "invalid tablet_meta key:" << key;
            return true;
        }
        return func(tablet_id, schema_hash, value);
    };
    return meta->iterate(META_COLUMN_FAMILY_INDEX, HEADER_PREFIX, traverse_header_func, timeout_sec);
}

std::string json_to_string(const rapidjson::Value& val_obj) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    val_obj.Accept(writer);
    return std::string(buf.GetString());
}

Status TabletMetaManager::build_primary_meta(DataDir* store, rapidjson::Document& doc, rocksdb::ColumnFamilyHandle* cf,
                                             rocksdb::WriteBatch& batch) {
    if (!doc["tablet_meta"].IsObject()) {
        return Status::InternalError("invalid json string");
    }
    rapidjson::Value& tablet_meta_obj = doc["tablet_meta"];
    TabletMetaPB tablet_meta_pb;
    bool ret = json2pb::JsonToProtoMessage(json_to_string(tablet_meta_obj), &tablet_meta_pb);
    if (!ret) {
        return Status::InternalError("parse tablet_meta_pb failed");
    }
    TTabletId tablet_id = tablet_meta_pb.tablet_id();
    TSchemaHash schema_hash = tablet_meta_pb.schema_hash();
    std::string key = encode_tablet_meta_key(tablet_id, schema_hash);
    std::string val = tablet_meta_pb.SerializeAsString();
    rocksdb::Status st = batch.Put(cf, key, val);
    if (!st.ok()) {
        return Status::IOError("rocksdb put tablet_meta failed");
    }

    // delete older data first
    if (!clear_log(store, &batch, tablet_id).ok()) {
        return Status::IOError("clear_log add to batch failed");
    }
    if (!clear_del_vector(store, &batch, tablet_id).ok()) {
        return Status::IOError("clear delvec add to batch failed");
    }
    if (!clear_delta_column_group(store, &batch, tablet_id).ok()) {
        return Status::IOError("clear dcg add to batch failed");
    }
    if (!clear_rowset(store, &batch, tablet_id).ok()) {
        return Status::IOError("clear rowset add to batch failed");
    }
    if (!clear_pending_rowset(store, &batch, tablet_id).ok()) {
        return Status::IOError("clear rowset add to batch failed");
    }

    if (doc.HasMember("applied_rs_metas") && doc["applied_rs_metas"].IsArray()) {
        rapidjson::Value& applied_rs_metas = doc["applied_rs_metas"];
        for (size_t i = 0; i < applied_rs_metas.Size(); ++i) {
            rapidjson::Value& rowset_meta = applied_rs_metas[i];
            RowsetMetaPB rowset_meta_pb;
            bool ret = json2pb::JsonToProtoMessage(json_to_string(rowset_meta), &rowset_meta_pb);
            if (!ret) {
                return Status::InternalError("parse rowset_meta_pb failed");
            }
            uint32_t rowset_seg_id = rowset_meta_pb.rowset_seg_id();
            auto key = encode_meta_rowset_key(tablet_id, rowset_seg_id);
            auto val = rowset_meta_pb.SerializeAsString();
            rocksdb::Status st = batch.Put(cf, key, val);
            if (!st.ok()) {
                return Status::IOError("rocksdb put rowset_meta failed");
            }
        }
    }

    if (doc.HasMember("pending_rs_metas") && doc["pending_rs_metas"].IsArray()) {
        rapidjson::Value& pending_rs_metas = doc["pending_rs_metas"];
        for (size_t i = 0; i < pending_rs_metas.Size(); ++i) {
            rapidjson::Value& rowset_meta = pending_rs_metas[i];
            int64_t version = rowset_meta["version"].GetInt64();
            auto key = encode_meta_pending_rowset_key(tablet_id, version);
            RowsetMetaPB rowset_meta_pb;
            bool ret = json2pb::JsonToProtoMessage(json_to_string(rowset_meta["rs_meta"]), &rowset_meta_pb);
            if (!ret) {
                return Status::InternalError("parse rowset_meta_pb failed");
            }
            auto val = rowset_meta_pb.SerializeAsString();
            rocksdb::Status st = batch.Put(cf, key, val);
            if (!st.ok()) {
                return Status::IOError("rocksdb put rowset_meta failed");
            }
        }
    }

    if (doc.HasMember("tablet_meta_logs") && doc["tablet_meta_logs"].IsArray()) {
        rapidjson::Value& tablet_meta_logs = doc["tablet_meta_logs"];
        for (size_t i = 0; i < tablet_meta_logs.Size(); ++i) {
            rapidjson::Value& meta_log = tablet_meta_logs[i];
            uint64_t logid = meta_log["logid"].GetUint64();
            auto key = encode_meta_log_key(tablet_id, logid);
            TabletMetaLogPB tablet_meta_log_pb;
            bool ret = json2pb::JsonToProtoMessage(json_to_string(meta_log["tablet_meta_log"]), &tablet_meta_log_pb);
            if (!ret) {
                return Status::InternalError("parse tablet_meta_log_pb failed");
            }
            auto val = tablet_meta_log_pb.SerializeAsString();
            rocksdb::Status st = batch.Put(cf, key, val);
            if (!st.ok()) {
                return Status::IOError("rocksdb put tablet_meta_log failed");
            }
        }
    }

    if (doc.HasMember("del_vectors") && doc["del_vectors"].IsArray()) {
        rapidjson::Value& del_vectors = doc["del_vectors"];
        for (size_t i = 0; i < del_vectors.Size(); ++i) {
            rapidjson::Value& del_vector = del_vectors[i];
            uint32_t segment_id = del_vector["segment_id"].GetUint();
            int64_t version = del_vector["version"].GetInt64();
            auto key = encode_del_vector_key(tablet_id, segment_id, version);
            std::string decode_val;
            base64_decode(del_vector["base64_val"].GetString(), &decode_val);
            rocksdb::Status st = batch.Put(cf, key, decode_val);
            if (!st.ok()) {
                return Status::IOError("rocksdb put del_vectors failed");
            }
        }
    }

    return Status::OK();
}

Status TabletMetaManager::load_json_meta(DataDir* store, const std::string& meta_path) {
    std::ifstream infile(meta_path);
    char buffer[102400];
    std::string json_meta;
    while (!infile.eof()) {
        infile.getline(buffer, 102400);
        json_meta = json_meta + buffer;
    }
    boost::algorithm::trim(json_meta);

    rocksdb::WriteBatch batch;
    rocksdb::ColumnFamilyHandle* cf = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    rapidjson::Document doc;
    if (doc.Parse(json_meta.data()).HasParseError()) {
        return Status::InternalError("parse json failed");
    }

    if (!doc.HasMember("tablet_meta")) {
        TabletMetaPB tablet_meta_pb;
        bool ret = json2pb::JsonToProtoMessage(json_meta, &tablet_meta_pb);
        if (!ret) {
            return Status::InternalError("parse tablet_meta failed");
        }
        TTabletId tablet_id = tablet_meta_pb.tablet_id();
        TSchemaHash schema_hash = tablet_meta_pb.schema_hash();
        std::string key = encode_tablet_meta_key(tablet_id, schema_hash);
        std::string val = tablet_meta_pb.SerializeAsString();
        rocksdb::Status st = batch.Put(cf, key, val);
        if (!st.ok()) {
            return Status::IOError("rocksdb put tablet_meta failed");
        }

        return store->get_meta()->write_batch(&batch);
    }

    Status st = build_primary_meta(store, doc, cf, batch);
    if (!st.ok()) {
        return st;
    }
    return store->get_meta()->write_batch(&batch);
}

static string encode_meta_log_key(TTabletId id, uint64_t logid) {
    string ret;
    ret.reserve(TABLET_META_LOG_PREFIX.length() + sizeof(uint64_t) * 2);
    ret.append(TABLET_META_LOG_PREFIX);
    put_fixed64_le(&ret, BigEndian::FromHost64(id));
    put_fixed64_le(&ret, BigEndian::FromHost64(logid));
    return ret;
}

static bool decode_meta_log_key(std::string_view key, TTabletId* id, uint64_t* logid) {
    if (key.length() != TABLET_META_LOG_PREFIX.length() + sizeof(uint64_t) * 2) {
        return false;
    }
    *id = BigEndian::ToHost64(UNALIGNED_LOAD64(key.data() + TABLET_META_LOG_PREFIX.length()));
    *logid = BigEndian::ToHost64(UNALIGNED_LOAD64(key.data() + TABLET_META_LOG_PREFIX.length() + sizeof(uint64_t)));
    return true;
}

static string encode_meta_rowset_key(TTabletId id, uint32_t rowsetid) {
    string ret;
    ret.reserve(TABLET_META_ROWSET_PREFIX.length() + sizeof(uint64_t) + sizeof(uint32_t));
    ret.append(TABLET_META_ROWSET_PREFIX);
    put_fixed64_le(&ret, BigEndian::FromHost64(id));
    put_fixed32_le(&ret, BigEndian::FromHost32(rowsetid));
    return ret;
}

[[maybe_unused]] static bool decode_meta_rowset_key(std::string_view key, TTabletId* id, uint32_t* rowsetid) {
    if (key.length() != TABLET_META_ROWSET_PREFIX.length() + sizeof(uint64_t) + sizeof(uint32_t)) {
        return false;
    }
    *id = BigEndian::ToHost64(UNALIGNED_LOAD64(key.data() + TABLET_META_ROWSET_PREFIX.length()));
    *rowsetid =
            BigEndian::ToHost32(UNALIGNED_LOAD32(key.data() + TABLET_META_ROWSET_PREFIX.length() + sizeof(uint64_t)));
    return true;
}

static string encode_meta_pending_rowset_key(TTabletId id, int64_t version) {
    string ret;
    ret.reserve(TABLET_META_PENDING_ROWSET_PREFIX.length() + sizeof(uint64_t) + sizeof(uint64_t));
    ret.append(TABLET_META_PENDING_ROWSET_PREFIX);
    put_fixed64_le(&ret, BigEndian::FromHost64(id));
    put_fixed64_le(&ret, BigEndian::FromHost64(version));
    return ret;
}

[[maybe_unused]] static bool decode_meta_pending_rowset_key(std::string_view key, TTabletId* id, int64_t* version) {
    if (key.length() != TABLET_META_PENDING_ROWSET_PREFIX.length() + sizeof(uint64_t) + sizeof(uint64_t)) {
        return false;
    }
    *id = BigEndian::ToHost64(UNALIGNED_LOAD64(key.data() + TABLET_META_PENDING_ROWSET_PREFIX.length()));
    *version = BigEndian::ToHost64(
            UNALIGNED_LOAD64(key.data() + TABLET_META_PENDING_ROWSET_PREFIX.length() + sizeof(uint64_t)));
    return true;
}

std::string encode_del_vector_key(TTabletId tablet_id, uint32_t segment_id, int64_t version) {
    std::string key;
    key.reserve(24);
    key.append(TABLET_DELVEC_PREFIX);
    put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    put_fixed32_le(&key, BigEndian::FromHost32(segment_id));
    // If a segment attached with multiple del-vectors, make them
    // sorted by version in reverse order in RocksDB.
    int64_t v = std::numeric_limits<int64_t>::max() - version;
    put_fixed64_le(&key, BigEndian::FromHost64(v));
    return key;
}

void decode_del_vector_key(std::string_view enc_key, TTabletId* tablet_id, uint32_t* segment_id, int64_t* version) {
    DCHECK_EQ(4 + sizeof(TTabletId) + sizeof(uint32_t) + sizeof(int64_t), enc_key.size());
    *tablet_id = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 4));
    *segment_id = BigEndian::ToHost32(UNALIGNED_LOAD32(enc_key.data() + 12));
    *version = INT64_MAX - BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 16));
}

std::string encode_persistent_index_key(TTabletId tablet_id) {
    std::string key;
    key.reserve(TABLET_PERSISTENT_INDEX_META_PREFIX.length() + sizeof(uint64_t));
    key.append(TABLET_PERSISTENT_INDEX_META_PREFIX);
    put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    return key;
}

void decode_persistent_index_key(std::string_view enc_key, TTabletId* tablet_id) {
    DCHECK_EQ(4 + sizeof(uint64_t), enc_key.size());
    *tablet_id = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 4));
}

int64_t decode_del_vector_key_version(std::string_view key) {
    DCHECK_GT(key.size(), sizeof(int64_t));
    auto v = UNALIGNED_LOAD64(key.data() + key.size() - sizeof(int64_t));
    return std::numeric_limits<int64_t>::max() - BigEndian::ToHost64(v);
}

std::string encode_delta_column_group_key(TTabletId tablet_id, uint32_t segment_id, int64_t version) {
    std::string key;
    key.reserve(24);
    key.append(TABLET_DELTA_COLUMN_GROUP_PREFIX);
    put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    put_fixed32_le(&key, BigEndian::FromHost32(segment_id));
    // If a segment attached with multiple delta column group, make them
    // sorted by version in reverse order in RocksDB.
    int64_t v = std::numeric_limits<int64_t>::max() - version;
    put_fixed64_le(&key, BigEndian::FromHost64(v));
    return key;
}

std::string encode_delta_column_group_key(TTabletId tablet_id, RowsetId rowsetid, uint32_t segment_id,
                                          int64_t version) {
    return encode_delta_column_group_key(tablet_id, rowsetid.to_string(), segment_id, version);
}

std::string encode_delta_column_group_key(TTabletId tablet_id, std::string rowsetid, uint32_t segment_id,
                                          int64_t version) {
    // max size of RowsetId string format < 64 byte
    rowsetid.resize(64, ' ');
    std::string key;
    key.reserve(88);
    key.append(TABLET_DELTA_COLUMN_GROUP_PREFIX);
    for (size_t i = 0; i < 8; ++i) {
        uint64_t* partial = reinterpret_cast<uint64_t*>(rowsetid.data() + i * 8);
        put_fixed64_le(&key, BigEndian::FromHost64(*partial));
    }
    put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    put_fixed32_le(&key, BigEndian::FromHost32(segment_id));
    // If a segment attached with multiple delta column group, make them
    // sorted by version in reverse order in RocksDB.
    int64_t v = std::numeric_limits<int64_t>::max() - version;
    put_fixed64_le(&key, BigEndian::FromHost64(v));
    return key;
}

void decode_delta_column_group_key(std::string_view enc_key, TTabletId* tablet_id, uint32_t* segment_id,
                                   int64_t* version) {
    DCHECK_EQ(4 + sizeof(TTabletId) + sizeof(uint32_t) + sizeof(int64_t), enc_key.size());
    *tablet_id = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 4));
    *segment_id = BigEndian::ToHost32(UNALIGNED_LOAD32(enc_key.data() + 12));
    *version = INT64_MAX - BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 16));
}

void decode_delta_column_group_key(std::string_view enc_key, TTabletId* tablet_id, std::string* rowsetid,
                                   uint32_t* segment_id, int64_t* version) {
    DCHECK_EQ(4 + 64 + sizeof(TTabletId) + sizeof(uint32_t) + sizeof(int64_t), enc_key.size());
    for (size_t i = 0; i < 8; ++i) {
        uint64_t partial = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 4 + i * 8));
        const char* str = reinterpret_cast<char*>(&partial);
        rowsetid->append(str, 8);
    }
    *tablet_id = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 68));
    *segment_id = BigEndian::ToHost32(UNALIGNED_LOAD32(enc_key.data() + 76));
    *version = INT64_MAX - BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 80));
}

Status TabletMetaManager::rowset_commit(DataDir* store, TTabletId tablet_id, int64_t logid, EditVersionMetaPB* edit,
                                        const RowsetMetaPB& rowset, const string& rowset_meta_key) {
    WriteBatch batch;
    auto handle = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    string logkey = encode_meta_log_key(tablet_id, logid);
    TabletMetaLogPB log;
    auto ops = log.add_ops();
    ops->set_type(TabletMetaOpType::OP_ROWSET_COMMIT);
    ops->set_allocated_commit(edit);
    auto logvalue = log.SerializeAsString();
    ops->release_commit();
    rocksdb::Status st = batch.Put(handle, logkey, logvalue);
    if (!st.ok()) {
        LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
        return to_status(st);
    }
    string rowsetkey = encode_meta_rowset_key(tablet_id, rowset.rowset_seg_id());
    auto rowsetvalue = rowset.SerializeAsString();
    st = batch.Put(handle, rowsetkey, rowsetvalue);
    if (!st.ok()) {
        LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
        return to_status(st);
    }
    if (!rowset_meta_key.empty()) {
        // delete rowset meta in txn
        st = batch.Delete(handle, rowset_meta_key);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.delete failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }
    // pending rowset may exists or not, but delete it anyway
    RETURN_IF_ERROR(delete_pending_rowset(store, &batch, tablet_id, edit->version().major()));
    return store->get_meta()->write_batch(&batch);
}

Status TabletMetaManager::write_persistent_index_meta(DataDir* store, TTabletId tablet_id,
                                                      const PersistentIndexMetaPB& meta) {
    WriteBatch batch;
    auto handle = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    string meta_key = encode_persistent_index_key(tablet_id);
    auto value = meta.SerializeAsString();
    rocksdb::Status st = batch.Put(handle, meta_key, value);
    if (!st.ok()) {
        LOG(WARNING) << "put persistent index meta failed, rocksdb.batch.put failed";
        return to_status(st);
    }
    return store->get_meta()->write_batch(&batch);
}

Status TabletMetaManager::rowset_delete(DataDir* store, TTabletId tablet_id, uint32_t rowset_id, uint32_t segments) {
    WriteBatch batch;
    KVStore* meta = store->get_meta();
    auto cf_meta = meta->handle(META_COLUMN_FAMILY_INDEX);

    auto st = batch.Delete(cf_meta, encode_meta_rowset_key(tablet_id, rowset_id));
    if (UNLIKELY(!st.ok())) {
        return to_status(st);
    }
    // Delete all delete vectors.
    if (segments > 0) {
        std::string lower = encode_del_vector_key(tablet_id, rowset_id + 0, INT64_MAX);
        std::string upper = encode_del_vector_key(tablet_id, rowset_id + segments, INT64_MAX);
        st = batch.DeleteRange(cf_meta, lower, upper);
        if (UNLIKELY(!st.ok())) {
            return Status::InternalError("remove delete vector failed");
        }
    }
    return meta->write_batch(&batch);
}

Status TabletMetaManager::rowset_iterate(DataDir* store, TTabletId tablet_id, const RowsetIterateFunc& func) {
    string prefix;
    prefix.reserve(TABLET_META_ROWSET_PREFIX.length() + sizeof(uint64_t));
    prefix.append(TABLET_META_ROWSET_PREFIX);
    put_fixed64_le(&prefix, BigEndian::FromHost64(tablet_id));

    return store->get_meta()->iterate(META_COLUMN_FAMILY_INDEX, prefix,
                                      [&](std::string_view key, std::string_view value) -> bool {
                                          bool parse_ok = false;
                                          auto rowset_meta = std::make_shared<RowsetMeta>(value, &parse_ok);
                                          CHECK(parse_ok) << "Corrupted rowset meta";
                                          return func(std::move(rowset_meta));
                                      });
}

Status TabletMetaManager::apply_rowset_commit(DataDir* store, TTabletId tablet_id, int64_t logid,
                                              const EditVersion& version,
                                              vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                              const PersistentIndexMetaPB& index_meta, bool enable_persistent_index,
                                              const RowsetMetaPB* rowset_meta) {
    auto span = Tracer::Instance().start_trace_tablet("apply_save_meta", tablet_id);
    span->SetAttribute("version", version.to_string());
    WriteBatch batch;
    auto handle = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    string logkey = encode_meta_log_key(tablet_id, logid);
    TabletMetaLogPB log;
    auto ops = log.add_ops();
    ops->set_type(TabletMetaOpType::OP_APPLY);
    auto version_pb = ops->mutable_apply();
    version_pb->set_major(version.major());
    version_pb->set_minor(version.minor());
    auto logval = log.SerializeAsString();
    rocksdb::Status st = batch.Put(handle, logkey, logval);
    if (!st.ok()) {
        LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
        return to_status(st);
    }
    TabletSegmentId tsid;
    tsid.tablet_id = tablet_id;
    span->AddEvent("delvec_start");
    int64_t total_bytes = 0;
    for (auto& rssid_delvec : delvecs) {
        tsid.segment_id = rssid_delvec.first;
        auto dv_key = encode_del_vector_key(tsid.tablet_id, tsid.segment_id, version.major());
        auto dv_value = rssid_delvec.second->save();
        total_bytes += dv_value.size();
        st = batch.Put(handle, dv_key, dv_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }
    span->SetAttribute("delvec_bytes", total_bytes);
    span->AddEvent("delvec_end");

    if (enable_persistent_index) {
        auto meta_key = encode_persistent_index_key(tsid.tablet_id);
        auto meta_value = index_meta.SerializeAsString();
        st = batch.Put(handle, meta_key, meta_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }

    if (rowset_meta != nullptr) {
        string rowset_key = encode_meta_rowset_key(tablet_id, rowset_meta->rowset_seg_id());
        auto rowset_value = rowset_meta->SerializeAsString();
        st = batch.Put(handle, rowset_key, rowset_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }

    return store->get_meta()->write_batch(&batch);
}

// used in column mode partial update
Status TabletMetaManager::apply_rowset_commit(DataDir* store, TTabletId tablet_id, int64_t logid,
                                              const EditVersion& version,
                                              const std::map<uint32_t, DeltaColumnGroupPtr>& delta_column_groups,
                                              const vector<std::pair<uint32_t, DelVectorPtr>>& delvecs,
                                              const PersistentIndexMetaPB& index_meta, bool enable_persistent_index,
                                              const starrocks::RowsetMetaPB* rowset_meta) {
    auto span = Tracer::Instance().start_trace_tablet("apply_save_meta", tablet_id);
    span->SetAttribute("version", version.to_string());
    WriteBatch batch;
    auto handle = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    string logkey = encode_meta_log_key(tablet_id, logid);
    TabletMetaLogPB log;
    auto ops = log.add_ops();
    ops->set_type(TabletMetaOpType::OP_APPLY);
    auto version_pb = ops->mutable_apply();
    version_pb->set_major(version.major());
    version_pb->set_minor(version.minor());
    auto logval = log.SerializeAsString();
    rocksdb::Status st = batch.Put(handle, logkey, logval);
    if (!st.ok()) {
        LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
        return to_status(st);
    }
    TabletSegmentId tsid;
    tsid.tablet_id = tablet_id;
    // persist delta column group
    span->AddEvent("delta_column_group_start");
    int64_t total_bytes = 0;
    for (const auto& delta_column : delta_column_groups) {
        tsid.segment_id = delta_column.first;
        auto dcg_key = encode_delta_column_group_key(tsid.tablet_id, tsid.segment_id, version.major());
        auto dcg_value = delta_column.second->save();
        total_bytes += dcg_value.size();
        st = batch.Put(handle, dcg_key, dcg_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }
    span->SetAttribute("delta_column_group_bytes", total_bytes);
    span->AddEvent("delta_column_group_end");
    // persist delvec
    span->AddEvent("delvec_start");
    for (auto& rssid_delvec : delvecs) {
        tsid.segment_id = rssid_delvec.first;
        auto dv_key = encode_del_vector_key(tsid.tablet_id, tsid.segment_id, version.major());
        auto dv_value = rssid_delvec.second->save();
        st = batch.Put(handle, dv_key, dv_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }
    span->AddEvent("delvec_end");

    if (enable_persistent_index) {
        auto meta_key = encode_persistent_index_key(tsid.tablet_id);
        auto meta_value = index_meta.SerializeAsString();
        st = batch.Put(handle, meta_key, meta_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }

    if (rowset_meta != nullptr) {
        string rowset_key = encode_meta_rowset_key(tablet_id, rowset_meta->rowset_seg_id());
        auto rowset_value = rowset_meta->SerializeAsString();
        st = batch.Put(handle, rowset_key, rowset_value);
        if (!st.ok()) {
            LOG(WARNING) << "rowset_commit failed, rocksdb.batch.put failed, tablet_id: " << tablet_id;
            return to_status(st);
        }
    }

    return store->get_meta()->write_batch(&batch);
}

Status TabletMetaManager::traverse_meta_logs(DataDir* store, TTabletId tablet_id,
                                             const std::function<bool(uint64_t, const TabletMetaLogPB&)>& func) {
    Status ret;
    std::string lower_bound = encode_meta_log_key(tablet_id, 0);
    std::string upper_bound = encode_meta_log_key(tablet_id, UINT64_MAX);

    auto st = store->get_meta()->iterate_range(META_COLUMN_FAMILY_INDEX, lower_bound, upper_bound,
                                               [&](std::string_view key, std::string_view value) {
                                                   TTabletId tid;
                                                   uint64_t logid;
                                                   if (!decode_meta_log_key(key, &tid, &logid)) {
                                                       ret = Status::Corruption("corrupted key of meta log");
                                                       return false;
                                                   }
                                                   DCHECK_EQ(tablet_id, tid);
                                                   TabletMetaLogPB log;
                                                   if (!log.ParseFromArray(value.data(), value.size())) {
                                                       ret = Status::Corruption("corrupted value of meta log");
                                                       return false;
                                                   }
                                                   return func(logid, log);
                                               });
    if (!st.ok()) {
        LOG(WARNING) << "Fail to iterate log, ret=" << st.to_string();
        ret = st;
    }
    return ret;
}

Status TabletMetaManager::set_del_vector(KVStore* meta, TTabletId tablet_id, uint32_t segment_id,
                                         const DelVector& delvec) {
    std::string key = encode_del_vector_key(tablet_id, segment_id, delvec.version());
    std::string val = delvec.save();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, val);
}

Status TabletMetaManager::get_del_vector(KVStore* meta, TTabletId tablet_id, uint32_t segment_id, int64_t version,
                                         DelVector* delvec, int64_t* latest_version) {
    std::string lower = encode_del_vector_key(tablet_id, segment_id, INT64_MAX);
    std::string upper = encode_del_vector_key(tablet_id, segment_id, 0);

    Status st;
    bool found = false;
    bool first = true;
    auto traverse_versions = [&](std::string_view key, std::string_view value) -> bool {
        int64_t cv = decode_del_vector_key_version(key);
        VLOG(3) << "traverse version got version: " << cv;
        if (first) {
            *latest_version = cv;
            first = false;
        }
        if (version >= cv) {
            st = delvec->load(cv, value.data(), value.size());
            found = true;
            return false;
        }
        return true;
    };
    st = meta->iterate_range(META_COLUMN_FAMILY_INDEX, lower, upper, traverse_versions);
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id << " segment_id=" << segment_id
                     << " error_code=" << st.to_string();
        return st;
    }
    if (!found) {
        return Status::NotFound(strings::Substitute("no delete vector found tablet:$0 segment:$1 version:$2", tablet_id,
                                                    segment_id, version));
    }
    VLOG(3) << strings::Substitute("get_del_vec in-meta tablet_id=$0 segment_id=$1 version=$2 actual_version=$3",
                                   tablet_id, segment_id, version, delvec ? delvec->version() : -1);
    return st;
}

Status TabletMetaManager::del_vector_iterate(KVStore* meta, TTabletId tablet_id, uint32_t lower, uint32_t upper,
                                             const std::function<bool(uint32_t, int64_t, std::string_view)>& func) {
    std::string lower_key = encode_del_vector_key(tablet_id, lower, INT64_MAX);
    std::string upper_key = encode_del_vector_key(tablet_id, upper, 0);

    Status st;
    st = meta->iterate_range(
            META_COLUMN_FAMILY_INDEX, lower_key, upper_key, [&](std::string_view key, std::string_view value) -> bool {
                TTabletId dummy;
                uint32_t segment_id;
                int64_t version;
                DelVectorPtr del_vec;
                decode_del_vector_key(key, &dummy, &segment_id, &version);
                DCHECK_EQ(tablet_id, dummy);
                if (!func(segment_id, version, value)) {
                    std::string msg = strings::Substitute("fail to get delvecs. tablet:$0 rowset:$1", tablet_id, lower);
                    LOG(WARNING) << msg;
                    st = Status::InternalError(msg);
                    return false;
                }
                return true;
            });
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id << " rowset=" << lower;
        return st;
    }
    return Status::OK();
}

using DeleteVectorList = TabletMetaManager::DeleteVectorList;
StatusOr<DeleteVectorList> TabletMetaManager::list_del_vector(KVStore* meta, TTabletId tablet_id, int64_t max_version) {
    DeleteVectorList ret;
    std::string lower = encode_del_vector_key(tablet_id, 0, INT64_MAX);
    std::string upper = encode_del_vector_key(tablet_id, UINT32_MAX, 0);
    uint32_t last_segment_id = UINT32_MAX;
    auto st = meta->iterate_range(META_COLUMN_FAMILY_INDEX, lower, upper,
                                  [&](std::string_view key, std::string_view value) -> bool {
                                      TTabletId dummy;
                                      uint32_t segment_id;
                                      int64_t version;
                                      decode_del_vector_key(key, &dummy, &segment_id, &version);
                                      DCHECK_EQ(tablet_id, dummy);
                                      if (segment_id != last_segment_id && version < max_version) {
                                          ret.emplace_back(segment_id, version);
                                          last_segment_id = segment_id;
                                      }
                                      return true;
                                  });
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id;
        return st;
    }
    return std::move(ret);
}

StatusOr<size_t> TabletMetaManager::delete_del_vector_before_version(KVStore* meta, TTabletId tablet_id,
                                                                     int64_t version) {
    DeleteVectorList ret;
    std::string lower = encode_del_vector_key(tablet_id, 0, INT64_MAX);
    std::string upper = encode_del_vector_key(tablet_id, UINT32_MAX, 0);
    std::map<uint32_t, std::vector<int64_t>> segments;
    auto st = meta->iterate_range(META_COLUMN_FAMILY_INDEX, lower, upper,
                                  [&](std::string_view key, std::string_view value) -> bool {
                                      TTabletId dummy;
                                      uint32_t segment_id;
                                      int64_t version;
                                      decode_del_vector_key(key, &dummy, &segment_id, &version);
                                      DCHECK_EQ(tablet_id, dummy);
                                      segments[segment_id].push_back(version);
                                      return true;
                                  });
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb for delete_del_vector_before_version. tablet_id=" << tablet_id;
        return st;
    }
    size_t num_delete = 0;
    WriteBatch batch;
    auto cf_handle = meta->handle(META_COLUMN_FAMILY_INDEX);
    std::ostringstream vlog_delvec_maplist;
    for (auto& segment : segments) {
        auto& versions = segment.second;
        bool del = false;
        bool added = false;
        for (long i : versions) {
            if (del) {
                std::string key = encode_del_vector_key(tablet_id, segment.first, i);
                rocksdb::Status st = batch.Delete(cf_handle, key);
                if (!st.ok()) {
                    return to_status(st);
                }
                num_delete++;
                if (!added) {
                    vlog_delvec_maplist << " " << segment.first << ":" << i;
                    added = true;
                } else {
                    vlog_delvec_maplist << "," << i;
                }
            } else if (i <= version) {
                // versions after this version can be deleted
                del = true;
            }
        }
    }
    RETURN_IF_ERROR(meta->write_batch(&batch));
    if (num_delete > 0) {
        LOG(INFO) << "delete_del_vector_before_version version:" << version << " tablet:" << tablet_id
                  << vlog_delvec_maplist.str();
    }
    return num_delete;
}

Status TabletMetaManager::delete_del_vector_range(KVStore* meta, TTabletId tablet_id, uint32_t segment_id,
                                                  int64_t start_version, int64_t end_version) {
    if (start_version == end_version) {
        return Status::OK();
    }
    if (start_version > end_version) {
        return Status::InvalidArgument("start version cannot greater than end version");
    }
    // Note that delete vectors are sorted by version in reverse order in RocksDB.
    std::string begin_key = encode_del_vector_key(tablet_id, segment_id, end_version - 1);
    std::string end_key = encode_del_vector_key(tablet_id, segment_id, start_version - 1);
    auto cf_handle = meta->handle(META_COLUMN_FAMILY_INDEX);
    WriteBatch batch;
    rocksdb::Status st = batch.DeleteRange(cf_handle, begin_key, end_key);
    if (!st.ok()) {
        return to_status(st);
    }
    return meta->write_batch(&batch);
}

Status TabletMetaManager::get_delta_column_group(KVStore* meta, TTabletId tablet_id, uint32_t segment_id,
                                                 int64_t version, DeltaColumnGroupList* dcgs) {
    return scan_delta_column_group(meta, tablet_id, segment_id, 0, version, dcgs);
}

Status TabletMetaManager::get_delta_column_group(KVStore* meta, TTabletId tablet_id, RowsetId rowsetid,
                                                 uint32_t segment_id, int64_t version, DeltaColumnGroupList* dcgs) {
    return scan_delta_column_group(meta, tablet_id, rowsetid, segment_id, 0, version, dcgs);
}

Status TabletMetaManager::scan_delta_column_group(KVStore* meta, TTabletId tablet_id, uint32_t segment_id,
                                                  int64_t begin_version, int64_t end_version,
                                                  DeltaColumnGroupList* dcgs) {
    std::string lower = encode_delta_column_group_key(tablet_id, segment_id, end_version);
    std::string upper = encode_delta_column_group_key(tablet_id, segment_id, begin_version);
    auto st = meta->iterate_range(META_COLUMN_FAMILY_INDEX, lower, upper,
                                  [&](std::string_view key, std::string_view value) -> bool {
                                      TTabletId dummy;
                                      uint32_t dummy_segment_id;
                                      int64_t decode_version;
                                      decode_delta_column_group_key(key, &dummy, &dummy_segment_id, &decode_version);
                                      CHECK(segment_id == dummy_segment_id);
                                      DeltaColumnGroupPtr dcg_ptr = std::make_shared<DeltaColumnGroup>();
                                      CHECK(dcg_ptr->load(decode_version, value.data(), value.size()).ok());
                                      CHECK(dcgs != nullptr);
                                      dcgs->push_back(std::move(dcg_ptr));
                                      return true;
                                  });
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id;
        return st;
    }
    return Status::OK();
}

Status TabletMetaManager::scan_delta_column_group(KVStore* meta, TTabletId tablet_id, RowsetId rowsetid,
                                                  uint32_t segment_id, int64_t begin_version, int64_t end_version,
                                                  DeltaColumnGroupList* dcgs) {
    std::string lower = encode_delta_column_group_key(tablet_id, rowsetid, segment_id, end_version);
    std::string upper = encode_delta_column_group_key(tablet_id, rowsetid, segment_id, begin_version);
    auto st = meta->iterate_range(
            META_COLUMN_FAMILY_INDEX, lower, upper, [&](std::string_view key, std::string_view value) -> bool {
                TTabletId dummy;
                uint32_t dummy_segment_id;
                int64_t decode_version;
                std::string rowsetid_string;
                std::string dummy_rowsetid_string;
                rowsetid_string = rowsetid.to_string();
                rowsetid_string.resize(64, ' ');
                decode_delta_column_group_key(key, &dummy, &dummy_rowsetid_string, &dummy_segment_id, &decode_version);
                CHECK(segment_id == dummy_segment_id);
                CHECK(rowsetid_string == dummy_rowsetid_string);
                DeltaColumnGroupPtr dcg_ptr = std::make_shared<DeltaColumnGroup>();
                CHECK(dcg_ptr->load(decode_version, value.data(), value.size()).ok());
                CHECK(dcgs != nullptr);
                dcgs->push_back(std::move(dcg_ptr));
                return true;
            });

    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id;
        return st;
    }
    return Status::OK();
}

Status TabletMetaManager::scan_tablet_delta_column_group(KVStore* meta, TTabletId tablet_id,
                                                         DeltaColumnGroupList* dcgs) {
    std::string lower = encode_delta_column_group_key(tablet_id, 0, INT64_MAX);
    std::string upper = encode_delta_column_group_key(tablet_id, UINT32_MAX, INT64_MAX);
    auto st = meta->iterate_range(META_COLUMN_FAMILY_INDEX, lower, upper,
                                  [&](std::string_view key, std::string_view value) -> bool {
                                      TTabletId dummy;
                                      uint32_t dummy_segment_id;
                                      int64_t decode_version;
                                      decode_delta_column_group_key(key, &dummy, &dummy_segment_id, &decode_version);
                                      DeltaColumnGroupPtr dcg_ptr = std::make_shared<DeltaColumnGroup>();
                                      CHECK(dcg_ptr->load(decode_version, value.data(), value.size()).ok());
                                      CHECK(dcgs != nullptr);
                                      dcgs->push_back(std::move(dcg_ptr));
                                      return true;
                                  });
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id;
        return st;
    }
    return Status::OK();
}

Status TabletMetaManager::scan_tablet_delta_column_group_by_segment(KVStore* meta, TTabletId tablet_id,
                                                                    std::map<uint32_t, DeltaColumnGroupList>* dcgs) {
    std::string lower = encode_delta_column_group_key(tablet_id, 0, INT64_MAX);
    std::string upper = encode_delta_column_group_key(tablet_id, UINT32_MAX, INT64_MAX);
    auto st = meta->iterate_range(META_COLUMN_FAMILY_INDEX, lower, upper,
                                  [&](std::string_view key, std::string_view value) -> bool {
                                      TTabletId dummy;
                                      uint32_t segment_id;
                                      int64_t decode_version;
                                      decode_delta_column_group_key(key, &dummy, &segment_id, &decode_version);
                                      DeltaColumnGroupPtr dcg_ptr = std::make_shared<DeltaColumnGroup>();
                                      CHECK(dcg_ptr->load(decode_version, value.data(), value.size()).ok());
                                      CHECK(dcgs != nullptr);
                                      (*dcgs)[segment_id].push_back(std::move(dcg_ptr));
                                      return true;
                                  });
    if (!st.ok()) {
        LOG(WARNING) << "fail to iterate rocksdb delvecs. tablet_id=" << tablet_id;
        return st;
    }
    return Status::OK();
}

Status TabletMetaManager::delete_delta_column_group(KVStore* meta, TTabletId tablet_id, uint32_t rowset_id,
                                                    uint32_t segments) {
    std::string lower = encode_delta_column_group_key(tablet_id, rowset_id, INT64_MAX);
    std::string upper = encode_delta_column_group_key(tablet_id, rowset_id + segments, INT64_MAX);
    auto h = meta->handle(META_COLUMN_FAMILY_INDEX);
    WriteBatch batch;
    rocksdb::Status st = batch.DeleteRange(h, lower, upper);
    if (!st.ok()) {
        return to_status(st);
    }
    return meta->write_batch(&batch);
}

Status TabletMetaManager::delete_delta_column_group(KVStore* meta, WriteBatch* batch, const TabletSegmentId& tsid,
                                                    int64_t version) {
    std::string key = encode_delta_column_group_key(tsid.tablet_id, tsid.segment_id, version);
    auto h = meta->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->Delete(h, key));
}

Status TabletMetaManager::delete_delta_column_group(KVStore* meta, TTabletId tablet_id, RowsetId rowsetid,
                                                    uint32_t segments) {
    std::string lower = encode_delta_column_group_key(tablet_id, rowsetid, 0, INT64_MAX);
    std::string upper = encode_delta_column_group_key(tablet_id, rowsetid, segments - 1, INT64_MAX);
    auto h = meta->handle(META_COLUMN_FAMILY_INDEX);
    WriteBatch batch;
    rocksdb::Status st = batch.DeleteRange(h, lower, upper);
    if (!st.ok()) {
        return to_status(st);
    }
    return meta->write_batch(&batch);
}

Status TabletMetaManager::put_rowset_meta(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                          const RowsetMetaPB& rowset_meta) {
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    auto k = encode_meta_rowset_key(tablet_id, rowset_meta.rowset_seg_id());
    auto v = rowset_meta.SerializeAsString();
    return to_status(batch->Put(h, k, v));
}

Status TabletMetaManager::put_del_vector(DataDir* store, WriteBatch* batch, TTabletId tablet_id, uint32_t segment_id,
                                         const DelVector& delvec) {
    auto k = encode_del_vector_key(tablet_id, segment_id, delvec.version());
    auto v = delvec.save();
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->Put(h, k, v));
}

Status TabletMetaManager::put_del_vectors(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                          const EditVersion& version,
                                          const vector<std::pair<uint32_t, DelVectorPtr>>& delvecs) {
    auto handle = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    TabletSegmentId tsid;
    tsid.tablet_id = tablet_id;
    for (auto& rssid_delvec : delvecs) {
        tsid.segment_id = rssid_delvec.first;
        auto dv_key = encode_del_vector_key(tsid.tablet_id, tsid.segment_id, version.major());
        auto dv_value = rssid_delvec.second->save();
        RETURN_IF_ERROR(batch->Put(handle, dv_key, dv_value));
    }
    return Status::OK();
}

Status TabletMetaManager::put_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                                 uint32_t segment_id, const DeltaColumnGroupList& dcgs) {
    for (const auto& dcg : dcgs) {
        auto k = encode_delta_column_group_key(tablet_id, segment_id, dcg->version());
        auto v = dcg->save();
        auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
        RETURN_IF_ERROR(to_status(batch->Put(h, k, v)));
    }
    return Status::OK();
}

Status TabletMetaManager::put_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                                 RowsetId rowsetid, uint32_t segment_id,
                                                 const DeltaColumnGroupList& dcgs) {
    return put_delta_column_group(store, batch, tablet_id, rowsetid.to_string(), segment_id, dcgs);
}

Status TabletMetaManager::put_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                                 const std::string& rowsetid, uint32_t segment_id,
                                                 const DeltaColumnGroupList& dcgs) {
    for (const auto& dcg : dcgs) {
        auto k = encode_delta_column_group_key(tablet_id, rowsetid, segment_id, dcg->version());
        auto v = dcg->save();
        auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
        RETURN_IF_ERROR(to_status(batch->Put(h, k, v)));
    }
    return Status::OK();
}

Status TabletMetaManager::put_tablet_meta(DataDir* store, WriteBatch* batch, const TabletMetaPB& meta) {
    auto k = encode_tablet_meta_key(meta.tablet_id(), meta.schema_hash());
    auto v = meta.SerializeAsString();
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->Put(h, k, v));
}

Status TabletMetaManager::clear_rowset(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    auto lower = encode_meta_rowset_key(tablet_id, 0);
    auto upper = encode_meta_rowset_key(tablet_id, UINT32_MAX);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->DeleteRange(h, lower, upper));
}

Status TabletMetaManager::clear_log(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    auto lower = encode_meta_log_key(tablet_id, 0);
    auto upper = encode_meta_log_key(tablet_id, UINT64_MAX);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->DeleteRange(h, lower, upper));
}

Status TabletMetaManager::clear_del_vector(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    auto lower = encode_del_vector_key(tablet_id, 0, INT64_MAX);
    auto upper = encode_del_vector_key(tablet_id, UINT32_MAX, INT64_MAX);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->DeleteRange(h, lower, upper));
}

Status TabletMetaManager::clear_delta_column_group(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    auto lower = encode_delta_column_group_key(tablet_id, 0, INT64_MAX);
    auto upper = encode_delta_column_group_key(tablet_id, UINT32_MAX, INT64_MAX);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->DeleteRange(h, lower, upper));
}

Status TabletMetaManager::clear_persistent_index(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    auto k = encode_persistent_index_key(tablet_id);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->Delete(h, k));
}

Status TabletMetaManager::remove_tablet_meta(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                             TSchemaHash schema_hash) {
    auto k = encode_tablet_meta_key(tablet_id, schema_hash);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->Delete(h, k));
}

Status TabletMetaManager::get_stats(DataDir* store, MetaStoreStats* stats, bool detail) {
    KVStore* meta = store->get_meta();

    auto traverse_tabletmeta_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        TSchemaHash thash;
        if (!decode_tablet_meta_key(key, &tid, &thash)) {
            LOG(WARNING) << "invalid tablet_meta key:" << key;
            stats->error_count++;
            return true;
        }
        TabletMetaPB tablet_meta_pb;
        bool parsed = tablet_meta_pb.ParseFromArray(value.data(), value.size());
        if (!parsed) {
            LOG(WARNING) << "bad tablet meta pb data tablet_id:" << tid;
            stats->error_count++;
            return true;
        }
        if (tablet_meta_pb.schema().keys_type() == KeysType::PRIMARY_KEYS) {
            stats->update_tablet_count++;
            stats->update_tablet_meta_bytes += value.size();
        } else {
            stats->tablet_count++;
            stats->tablet_meta_bytes += value.size();
        }
        if (detail) {
            if (stats->tablets.find(tid) != stats->tablets.end()) {
                LOG(WARNING) << "found duplicate tablet meta tablet_id:" << tid << " schema_hash:" << thash;
                stats->error_count++;
            }
            TabletMetaStats ts;
            ts.tablet_id = tid;
            ts.table_id = tablet_meta_pb.table_id();
            ts.tablet_meta_bytes = value.size();
            stats->tablets[tid] = ts;
        }
        return true;
    };
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, HEADER_PREFIX, traverse_tabletmeta_func));
    stats->total_count += stats->tablet_count;
    stats->total_meta_bytes += stats->tablet_meta_bytes;
    stats->total_count += stats->update_tablet_count;
    stats->total_meta_bytes += stats->update_tablet_meta_bytes;

    RowsetMetaManager::traverse_rowset_metas(
            meta, [&](const TabletUid& tablet_uid, const RowsetId& rowset_id, std::string_view value) -> bool {
                stats->rowset_count++;
                stats->rowset_meta_bytes += value.size();
                if (detail) {
                    bool parsed = false;
                    auto rowset_meta = std::make_shared<RowsetMeta>(value, &parsed);
                    if (!parsed) {
                        LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
                        return true;
                    }
                    if (rowset_meta->tablet_uid() != tablet_uid) {
                        LOG(WARNING) << "tablet uid is not equal, skip the rowset"
                                     << ", rowset_id=" << rowset_meta->rowset_id()
                                     << ", in_put_tablet_uid=" << tablet_uid
                                     << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
                        return true;
                    }
                    auto itr = stats->tablets.find(rowset_meta->tablet_id());
                    if (itr == stats->tablets.end()) {
                        // reduce print warning log here, cause there may be many orphan rowsets
                        stats->error_count++;
                        LOG_EVERY_SECOND(WARNING)
                                << "rst_ rowset without tablet tablet_id:" << rowset_meta->tablet_id()
                                << " rowset_id:" << rowset_meta->rowset_id() << " version:" << rowset_meta->version();
                    } else {
                        itr->second.rowset_count++;
                        itr->second.rowset_meta_bytes += value.size();
                    }
                }
                return true;
            });
    stats->total_count += stats->rowset_count;
    stats->total_meta_bytes += stats->rowset_meta_bytes;

    auto traverse_log_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        uint64_t logid;
        if (!decode_meta_log_key(key, &tid, &logid)) {
            LOG(WARNING) << "invalid tablet_meta_log key:" << key;
            stats->error_count++;
            return true;
        }
        stats->log_count++;
        stats->log_meta_bytes += value.size();
        if (detail) {
            auto itr = stats->tablets.find(tid);
            if (itr == stats->tablets.end()) {
                LOG(WARNING) << "tablet_meta_log without tablet tablet_id:" << tid << " logid:" << logid;
                stats->error_count++;
            } else {
                itr->second.log_count++;
                itr->second.log_meta_bytes += value.size();
            }
        }
        return true;
    };
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, TABLET_META_LOG_PREFIX, traverse_log_func));
    stats->total_count += stats->log_count;
    stats->total_meta_bytes += stats->log_meta_bytes;

    auto traverse_delvec_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        uint32_t rssid;
        int64_t version;
        decode_del_vector_key(key, &tid, &rssid, &version);
        stats->delvec_count++;
        stats->delvec_meta_bytes += value.size();
        if (detail) {
            auto itr = stats->tablets.find(tid);
            if (itr == stats->tablets.end()) {
                LOG(WARNING) << "tablet_delvec without tablet tablet_id:" << tid << " rssid:" << rssid
                             << " version:" << version;
                stats->error_count++;
            } else {
                itr->second.delvec_count++;
                itr->second.delvec_meta_bytes += value.size();
            }
        }
        return true;
    };
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, TABLET_DELVEC_PREFIX, traverse_delvec_func));
    stats->total_count += stats->delvec_count;
    stats->total_meta_bytes += stats->delvec_meta_bytes;

    auto traverse_rowset_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        uint32_t rowsetid;
        if (!decode_meta_rowset_key(key, &tid, &rowsetid)) {
            LOG(WARNING) << "invalid rowsetid key:" << key;
            stats->error_count++;
            return true;
        }
        stats->update_rowset_count++;
        stats->update_rowset_meta_bytes += value.size();
        if (detail) {
            auto itr = stats->tablets.find(tid);
            if (itr == stats->tablets.end()) {
                LOG(WARNING) << "tablet_rowset without tablet tablet_id:" << tid << " rowsetid:" << rowsetid;
                stats->error_count++;
            } else {
                itr->second.rowset_count++;
                itr->second.rowset_meta_bytes += value.size();
            }
        }
        return true;
    };
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, TABLET_META_ROWSET_PREFIX, traverse_rowset_func));
    stats->total_count += stats->update_rowset_count;
    stats->total_meta_bytes += stats->update_rowset_meta_bytes;

    auto traverse_pending_rowset_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        int64_t version;
        if (!decode_meta_pending_rowset_key(key, &tid, &version)) {
            LOG(WARNING) << "invalid pending rowsetid key:" << key;
            return true;
        }
        stats->pending_rowset_count++;
        stats->pending_rowset_meta_bytes += value.size();
        if (detail) {
            auto itr = stats->tablets.find(tid);
            if (itr == stats->tablets.end()) {
                LOG(WARNING) << "pending_rowset without tablet tablet_id:" << tid << " version:" << version;
                stats->error_count++;
            } else {
                itr->second.pending_rowset_count++;
                itr->second.pending_rowset_meta_bytes += value.size();
            }
        }
        return true;
    };
    RETURN_IF_ERROR(
            meta->iterate(META_COLUMN_FAMILY_INDEX, TABLET_META_PENDING_ROWSET_PREFIX, traverse_pending_rowset_func));
    stats->total_count += stats->pending_rowset_count;
    stats->total_meta_bytes += stats->pending_rowset_meta_bytes;

    return Status::OK();
}

Status TabletMetaManager::remove_primary_key_meta(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    if (!clear_log(store, batch, tablet_id).ok()) {
        LOG(WARNING) << "clear_log add to batch failed";
    }
    if (!clear_del_vector(store, batch, tablet_id).ok()) {
        LOG(WARNING) << "clear delvec add to batch failed";
    }
    if (!clear_delta_column_group(store, batch, tablet_id).ok()) {
        LOG(WARNING) << "clear dcg add to batch failed";
    }
    if (!clear_rowset(store, batch, tablet_id).ok()) {
        LOG(WARNING) << "clear rowset add to batch failed";
    }
    if (!clear_pending_rowset(store, batch, tablet_id).ok()) {
        LOG(WARNING) << "clear rowset add to batch failed";
    }
    if (!clear_persistent_index(store, batch, tablet_id).ok()) {
        LOG(WARNING) << "clear persistent index add to batch failed";
    }
    return Status::OK();
}

Status TabletMetaManager::remove(DataDir* store, TTabletId tablet_id) {
    KVStore* meta = store->get_meta();
    WriteBatch batch;
    bool is_primary = false;
    rocksdb::ColumnFamilyHandle* cf = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    auto traverse_tabletmeta_func = [&](std::string_view key, std::string_view value) -> bool {
        TTabletId tid;
        TSchemaHash thash;
        if (!decode_tablet_meta_key(key, &tid, &thash)) {
            LOG(WARNING) << "invalid tablet_meta key:" << key;
            return false;
        }
        if (tid == tablet_id) {
            auto st = batch.Delete(cf, string(key));
            if (!st.ok()) {
                LOG(WARNING) << "batch.Delete failed, key:" << key;
            }
            TabletMetaPB tablet_meta_pb;
            bool parsed = tablet_meta_pb.ParseFromArray(value.data(), value.size());
            if (!parsed) {
                LOG(WARNING) << "bad tablet meta pb data tablet_id:" << tid;
            } else {
                is_primary = tablet_meta_pb.schema().keys_type() == KeysType::PRIMARY_KEYS;
            }
            return true;
        } else {
            return false;
        }
    };
    string prefix = strings::Substitute("$0$1_", HEADER_PREFIX, tablet_id);
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, traverse_tabletmeta_func));
    if (is_primary) {
        remove_primary_key_meta(store, &batch, tablet_id);
    }
    return meta->write_batch(&batch);
}

Status TabletMetaManager::remove_table_meta(DataDir* store, TTableId table_id) {
    KVStore* meta = store->get_meta();
    WriteBatch batch;
    bool is_primary = false;
    std::vector<int64_t> delete_tablet;
    rocksdb::ColumnFamilyHandle* cf = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    auto traverse_tabletmeta_func = [&](std::string_view key, std::string_view value) -> bool {
        TabletMetaPB tablet_meta_pb;
        bool parsed = tablet_meta_pb.ParseFromArray(value.data(), value.size());
        if (!parsed) {
            LOG(WARNING) << "bad tablet meta pb data, tablet_meta key: " << key;
        } else {
            is_primary = tablet_meta_pb.schema().keys_type() == KeysType::PRIMARY_KEYS;
            if (tablet_meta_pb.table_id() == table_id) {
                auto st = batch.Delete(cf, string(key));
                if (!st.ok()) {
                    LOG(WARNING) << "batch.Delete failed, key:" << key;
                } else if (is_primary) {
                    remove_primary_key_meta(store, &batch, tablet_meta_pb.tablet_id());
                }
            }
        }
        return true;
    };
    meta->iterate(META_COLUMN_FAMILY_INDEX, HEADER_PREFIX, traverse_tabletmeta_func);
    return meta->write_batch(&batch);
}

Status TabletMetaManager::remove_table_persistent_index_meta(DataDir* store, TTableId table_id) {
    KVStore* meta = store->get_meta();
    WriteBatch batch;
    auto traverse_tabletmeta_func = [&](std::string_view key, std::string_view value) -> bool {
        TabletMetaPB tablet_meta_pb;
        bool parsed = tablet_meta_pb.ParseFromArray(value.data(), value.size());
        if (!parsed) {
            LOG(WARNING) << "bad tablet meta pb data, tablet_meta key: " << key;
        } else {
            if (tablet_meta_pb.table_id() == table_id) {
                if (!clear_persistent_index(store, &batch, tablet_meta_pb.tablet_id()).ok()) {
                    LOG(WARNING) << "clear persistent index add to batch failed";
                }
            }
        }
        return true;
    };
    meta->iterate(META_COLUMN_FAMILY_INDEX, HEADER_PREFIX, traverse_tabletmeta_func);
    return meta->write_batch(&batch);
}

Status TabletMetaManager::remove_tablet_persistent_index_meta(DataDir* store, TTabletId tablet_id) {
    KVStore* meta = store->get_meta();
    WriteBatch batch;
    RETURN_IF_ERROR(clear_persistent_index(store, &batch, tablet_id));
    return meta->write_batch(&batch);
}

// methods for operating pending commits
Status TabletMetaManager::pending_rowset_commit(DataDir* store, TTabletId tablet_id, int64_t version,
                                                const RowsetMetaPB& rowset, const string& rowset_meta_key) {
    // remove rowset_meta_key, add pending rowset
    WriteBatch batch;
    auto handle = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    if (!rowset_meta_key.empty()) {
        // delete rowset meta in txn
        auto st = batch.Delete(handle, rowset_meta_key);
        if (!st.ok()) {
            LOG(WARNING) << "pending_rowset_commit, rocksdb.batch.delete failed";
            return to_status(st);
        }
    }
    auto pkey = encode_meta_pending_rowset_key(tablet_id, version);
    auto pvalue = rowset.SerializeAsString();
    auto st = batch.Put(handle, pkey, pvalue);
    if (!st.ok()) {
        LOG(WARNING) << "pending_rowset_commit, rocksdb.batch.put failed";
        return to_status(st);
    }
    return store->get_meta()->write_batch(&batch);
}

Status TabletMetaManager::pending_rowset_iterate(DataDir* store, TTabletId tablet_id,
                                                 const TabletMetaManager::PendingRowsetIterateFunc& func) {
    string prefix;
    prefix.reserve(TABLET_META_PENDING_ROWSET_PREFIX.length() + sizeof(uint64_t));
    prefix.append(TABLET_META_PENDING_ROWSET_PREFIX);
    put_fixed64_le(&prefix, BigEndian::FromHost64(tablet_id));

    return store->get_meta()->iterate(
            META_COLUMN_FAMILY_INDEX, prefix, [&](std::string_view key, std::string_view value) -> bool {
                TTabletId tid = -1;
                int64_t version = -1;
                if (!decode_meta_pending_rowset_key(key, &tid, &version)) {
                    LOG(ERROR) << "corrupt pending rowset key: " << hexdump(key.data(), key.length());
                    return false;
                }
                return func(version, value);
            });
}

Status TabletMetaManager::delete_pending_rowset(DataDir* store, WriteBatch* batch, TTabletId tablet_id,
                                                int64_t version) {
    auto pkey = encode_meta_pending_rowset_key(tablet_id, version);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->Delete(h, pkey));
}

Status TabletMetaManager::delete_pending_rowset(DataDir* store, TTabletId tablet_id, int64_t version) {
    auto pkey = encode_meta_pending_rowset_key(tablet_id, version);
    return store->get_meta()->remove(META_COLUMN_FAMILY_INDEX, pkey);
}

Status TabletMetaManager::clear_pending_rowset(DataDir* store, WriteBatch* batch, TTabletId tablet_id) {
    auto lower = encode_meta_pending_rowset_key(tablet_id, 0);
    auto upper = encode_meta_pending_rowset_key(tablet_id, INT64_MAX);
    auto h = store->get_meta()->handle(META_COLUMN_FAMILY_INDEX);
    return to_status(batch->DeleteRange(h, lower, upper));
}

} // namespace starrocks
