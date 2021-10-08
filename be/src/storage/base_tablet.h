// This file is made available under Elastic License 2.0.
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

#ifndef STARROCKS_BE_SRC_OLAP_BASE_TABLET_H
#define STARROCKS_BE_SRC_OLAP_BASE_TABLET_H

#include <memory>

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
    BaseTablet(MemTracker* mem_tracker, const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir);
    virtual ~BaseTablet() = default;

    inline DataDir* data_dir() const;
    const std::string& tablet_path() const;

    TabletState tablet_state() const { return _state; }
    OLAPStatus set_tablet_state(TabletState state);

    // Property encapsulated in TabletMeta
    inline const TabletMetaSharedPtr tablet_meta();

    inline TabletUid tablet_uid() const;
    inline int64_t table_id() const;
    // Returns a string can be used to uniquely identify a tablet.
    // The result string will often be printed to the log.
    inline const std::string full_name() const;
    inline int64_t partition_id() const;
    inline int64_t tablet_id() const;
    inline int32_t schema_hash() const;
    inline int16_t shard_id();
    inline const int64_t creation_time() const;
    inline void set_creation_time(int64_t creation_time);
    inline bool equal(int64_t tablet_id, int32_t schema_hash);

    // properties encapsulated in TabletSchema
    inline const TabletSchema& tablet_schema() const;

    inline MemTracker* mem_tracker() { return _mem_tracker; }

protected:
    virtual void on_shutdown() {}

    void _gen_tablet_path();

    MemTracker* _mem_tracker = nullptr;

    TabletState _state;
    TabletMetaSharedPtr _tablet_meta;

    DataDir* _data_dir;
    std::string _tablet_path;

private:
    BaseTablet(const BaseTablet&) = delete;
    const BaseTablet& operator=(const BaseTablet&) = delete;
};

inline DataDir* BaseTablet::data_dir() const {
    return _data_dir;
}

inline const std::string& BaseTablet::tablet_path() const {
    return _tablet_path;
}

inline const TabletMetaSharedPtr BaseTablet::tablet_meta() {
    return _tablet_meta;
}

inline TabletUid BaseTablet::tablet_uid() const {
    return _tablet_meta->tablet_uid();
}

inline int64_t BaseTablet::table_id() const {
    return _tablet_meta->table_id();
}

inline const std::string BaseTablet::full_name() const {
    std::stringstream ss;
    ss << _tablet_meta->tablet_id() << "." << _tablet_meta->schema_hash() << "."
       << _tablet_meta->tablet_uid().to_string();
    return ss.str();
}

inline int64_t BaseTablet::partition_id() const {
    return _tablet_meta->partition_id();
}

inline int64_t BaseTablet::tablet_id() const {
    return _tablet_meta->tablet_id();
}

inline int32_t BaseTablet::schema_hash() const {
    return _tablet_meta->schema_hash();
}

inline int16_t BaseTablet::shard_id() {
    return _tablet_meta->shard_id();
}

inline const int64_t BaseTablet::creation_time() const {
    return _tablet_meta->creation_time();
}

inline void BaseTablet::set_creation_time(int64_t creation_time) {
    _tablet_meta->set_creation_time(creation_time);
}

inline bool BaseTablet::equal(int64_t id, int32_t hash) {
    return (tablet_id() == id) && (schema_hash() == hash);
}

inline const TabletSchema& BaseTablet::tablet_schema() const {
    return _tablet_meta->tablet_schema();
}

} /* namespace starrocks */

#endif /* STARROCKS_BE_SRC_OLAP_BASE_TABLET_H */
