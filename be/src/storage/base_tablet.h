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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/base_tablet.h

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

#pragma once

#include <memory>

#include "storage/data_dir.h"
#include "storage/olap_define.h"
#include "storage/tablet_meta.h"
#include "storage/utils.h"

namespace starrocks {

class DataDir;

// Base class for all tablet classes, currently only olap/Tablet and
// olap/memory/MemTablet.
// The fields and methods in this class is not final, it will change as memory
// storage engine evolves.
class BaseTablet : public std::enable_shared_from_this<BaseTablet> {
public:
    BaseTablet(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir);
    // for ut
    BaseTablet() = default;

    virtual ~BaseTablet() = default;

    DataDir* data_dir() const;

    void set_data_dir(DataDir* data_dir) { _data_dir = data_dir; }

    // A tablet's data are stored in disk files under a directory with structure like:
    //   ${storage_root_path}/${shard_number}/${tablet_id}/${schema_hash}
    //
    // The reason why create a directory ${schema_hash} under the directory ${tablet_id} is that in some very
    // earlier versions of Doris(https://github.com/apache/incubator-doris), it's possible to have multiple
    // tablets with the same tablet id but different schema hash, therefore the schema hash is constructed as
    // part of the data path of a tablet to distinguish each other.
    // The design that multiple tablets can share the same tablet id has been dropped by Doris, but the directory
    // structure has been kept.
    // Since StarRocks is built on earlier work on Doris, this directory structure has been kept in StarRocks too.
    //
    // `schema_hash_path()` returns the full path of the directory ${schema_hash}.
    // `tablet_id_path()` returns the full path of the directory ${tablet_id}.
    const std::string& schema_hash_path() const;

    std::string tablet_id_path() const;

    TabletState tablet_state() const { return _state; }
    Status set_tablet_state(TabletState state);

    // meta lock
    void obtain_header_rdlock() { _meta_lock.lock_shared(); }
    void obtain_header_wrlock() { _meta_lock.lock(); }
    void release_header_lock() { _meta_lock.unlock(); }
    std::shared_mutex& get_header_lock() { return _meta_lock; }

    // Property encapsulated in TabletMeta
    const TabletMetaSharedPtr tablet_meta();

    void set_tablet_meta(const TabletMetaSharedPtr& tablet_meta) { _tablet_meta = tablet_meta; }

    TabletUid tablet_uid() const;
    int64_t belonged_table_id() const;
    // Returns a string can be used to uniquely identify a tablet.
    // The result string will often be printed to the log.
    const std::string full_name() const;
    int64_t partition_id() const;
    int64_t tablet_id() const;
    int32_t schema_hash() const;
    int16_t shard_id();
    const int64_t creation_time() const;
    void set_creation_time(int64_t creation_time);
    bool equal(int64_t tablet_id, int32_t schema_hash);

    // properties encapsulated in TabletSchema
    const TabletSchema& tablet_schema() const;

    bool is_used();

    void register_tablet_into_dir();
    void deregister_tablet_from_dir();

    // propreties encapsulated in TabletSchema
    KeysType keys_type() const;
    size_t num_columns() const;
    size_t num_key_columns() const;
    size_t num_rows_per_row_block() const;
    size_t next_unique_id() const;
    size_t field_index(const string& field_name) const;

    const DelPredicateArray& delete_predicates() const;

    bool get_enable_persistent_index();

    void set_enable_persistent_index_unlocked(bool enable_persistent_index);

    void set_binlog_config_unlocked(TBinlogConfig binlog_config);

    Status set_partition_id_unlocked(int64_t partition_id);

    // should save tablet meta to remote meta store
    // if it's a primary replica
    void save_meta_unlocked();

protected:
    virtual void on_shutdown() {}

    void _gen_tablet_path();

    TabletState _state;
    TabletMetaSharedPtr _tablet_meta;
    mutable std::shared_mutex _meta_lock;

    DataDir* _data_dir;
    std::string _tablet_path; // TODO: remove this variable for less memory occupation

private:
    BaseTablet(const BaseTablet&) = delete;
    const BaseTablet& operator=(const BaseTablet&) = delete;
};

inline DataDir* BaseTablet::data_dir() const {
    return _data_dir;
}

inline const std::string& BaseTablet::schema_hash_path() const {
    return _tablet_path;
}

inline const TabletMetaSharedPtr BaseTablet::tablet_meta() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta;
}

inline TabletUid BaseTablet::tablet_uid() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_uid();
}

inline int64_t BaseTablet::belonged_table_id() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->table_id();
}

inline const std::string BaseTablet::full_name() const {
    std::shared_lock rdlock(_meta_lock);
    std::stringstream ss;
    ss << _tablet_meta->tablet_id() << "." << _tablet_meta->schema_hash() << "."
       << _tablet_meta->tablet_uid().to_string();
    return ss.str();
}

inline int64_t BaseTablet::partition_id() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->partition_id();
}

inline int64_t BaseTablet::tablet_id() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_id();
}

inline int32_t BaseTablet::schema_hash() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->schema_hash();
}

inline int16_t BaseTablet::shard_id() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->shard_id();
}

inline const int64_t BaseTablet::creation_time() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->creation_time();
}

inline void BaseTablet::set_creation_time(int64_t creation_time) {
    std::shared_lock rdlock(_meta_lock);
    _tablet_meta->set_creation_time(creation_time);
}

inline bool BaseTablet::equal(int64_t id, int32_t hash) {
    return tablet_id() == id && schema_hash() == hash;
}

inline const TabletSchema& BaseTablet::tablet_schema() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema();
}

inline bool BaseTablet::is_used() {
    return _data_dir->is_used();
}

inline void BaseTablet::register_tablet_into_dir() {
    _data_dir->register_tablet(this);
}

inline void BaseTablet::deregister_tablet_from_dir() {
    _data_dir->deregister_tablet(this);
}

inline KeysType BaseTablet::keys_type() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema().keys_type();
}

inline size_t BaseTablet::num_columns() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema().num_columns();
}

inline size_t BaseTablet::num_key_columns() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema().num_key_columns();
}

inline size_t BaseTablet::num_rows_per_row_block() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema().num_rows_per_row_block();
}

inline size_t BaseTablet::next_unique_id() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema().next_column_unique_id();
}

inline size_t BaseTablet::field_index(const string& field_name) const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_schema().field_index(field_name);
}

inline const DelPredicateArray& BaseTablet::delete_predicates() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->delete_predicates();
}

inline bool BaseTablet::get_enable_persistent_index() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->get_enable_persistent_index();
}

inline void BaseTablet::set_enable_persistent_index_unlocked(bool enable_persistent_index) {
    return _tablet_meta->set_enable_persistent_index(enable_persistent_index);
}

inline void BaseTablet::set_binlog_config_unlocked(TBinlogConfig binlog_config) {
    _tablet_meta->set_binlog_config(binlog_config);
}

inline Status BaseTablet::set_partition_id_unlocked(int64_t partition_id) {
    return _tablet_meta->set_partition_id(partition_id);
}

// should save tablet meta to remote meta store
// if it's a primary replica
inline void BaseTablet::save_meta_unlocked() {
    auto st = _tablet_meta->save_meta(_data_dir);
    CHECK(st.ok()) << "fail to save tablet_meta: " << st;
}

} /* namespace starrocks */
