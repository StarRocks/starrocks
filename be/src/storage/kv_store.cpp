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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_meta.cpp

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

#include "storage/kv_store.h"

#include <sstream>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "storage/olap_define.h"
#include "storage/rocksdb_status_adapter.h"
#include "util/runtime_profile.h"
#include "util/starrocks_metrics.h"

using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;
using rocksdb::Iterator;
using rocksdb::kDefaultColumnFamilyName;
using rocksdb::NewFixedPrefixTransform;

namespace starrocks {
const std::string META_POSTFIX = "/meta"; // NOLINT
const std::string SECOND_POSTFIX = "_secondary";
const size_t PREFIX_LENGTH = 4;

KVStore::KVStore(std::string root_path) : _root_path(std::move(root_path)), _db(nullptr) {}

KVStore::~KVStore() {
    for (auto& handle : _handles) {
        delete handle;
    }
    if (_db != nullptr) {
        _db->Close();
        delete _db;
        _db = nullptr;
    }
}

Status KVStore::init(bool read_only) {
    DBOptions options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    std::string db_path = _root_path + META_POSTFIX;

    ColumnFamilyOptions meta_cf_options;
    RETURN_IF_ERROR(rocksdb::GetColumnFamilyOptionsFromString(meta_cf_options, config::rocksdb_cf_options_string,
                                                              &meta_cf_options));
    // The index of each column family must be consistent with the enum `ColumnFamilyIndex`
    // defined in olap_define.h
    std::vector<ColumnFamilyDescriptor> cf_descs(NUM_COLUMN_FAMILY_INDEX);
    cf_descs[0].name = DEFAULT_COLUMN_FAMILY;
    cf_descs[0].options.compression = rocksdb::kSnappyCompression;
    cf_descs[1].name = STARROCKS_COLUMN_FAMILY;
    cf_descs[1].options.compression = rocksdb::kSnappyCompression;
    cf_descs[2].name = META_COLUMN_FAMILY;
    cf_descs[2].options = meta_cf_options;
    cf_descs[2].options.prefix_extractor.reset(NewFixedPrefixTransform(PREFIX_LENGTH));
    cf_descs[2].options.compression = rocksdb::kSnappyCompression;
    cf_descs[3].name = TXN_COLUMN_FAMILY;
    cf_descs[3].options.compression = rocksdb::kSnappyCompression;
    static_assert(NUM_COLUMN_FAMILY_INDEX == 4);

    rocksdb::Status s;
    if (read_only) {
        std::string secondary_path = db_path + SECOND_POSTFIX;
        s = DB::OpenAsSecondary(options, db_path, secondary_path, cf_descs, &_handles, &_db);
    } else {
        s = DB::Open(options, db_path, cf_descs, &_handles, &_db);
    }
    if (s.ok() && _db != nullptr) {
        return Status::OK();
    }

    LOG(WARNING) << "Fail to open RocksDB, reason:" << s.ToString() << ", path:" << db_path;

    //
    // Open failed, may be it's because the column families we are trying to open is a subset of column families,
    // this may happen in the case that StarRocks has been upgrated to a newer version with some extra column
    // families created, but rolled back the the current version again. So, here we try to get all column families
    // and open the database again.
    std::vector<std::string> cf_names;
    s = DB::ListColumnFamilies(options, db_path, &cf_names);
    if (!s.ok()) {
        LOG(WARNING) << "Fail to list column families, reason:" << s.ToString();
        return to_status(s);
    }
    // Erase all pre-defined column families from |cf_names| to check whether the RocksDB
    // contains some unknown column families.
    std::set<std::string> unknown_cf_names(cf_names.begin(), cf_names.end());
    unknown_cf_names.erase(DEFAULT_COLUMN_FAMILY);
    unknown_cf_names.erase(STARROCKS_COLUMN_FAMILY);
    unknown_cf_names.erase(META_COLUMN_FAMILY);

    // Assume these unknown column families were created with the default options.
    for (const auto& unknown_cf : unknown_cf_names) {
        LOG(WARNING) << "unknown column family '" << unknown_cf << "'";
        cf_descs.emplace_back(unknown_cf, ColumnFamilyOptions());
    }
    s = DB::Open(options, db_path, cf_descs, &_handles, &_db);
    if (s.ok() && _db != nullptr) {
        return Status::OK();
    }
    LOG(WARNING) << "Fail to open rocksdb, reason:" << s.ToString();
    return to_status(s);
}

Status KVStore::get(ColumnFamilyIndex column_family_index, const std::string& key, std::string* value) {
    StarRocksMetrics::instance()->meta_read_request_total.increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    rocksdb::Status s;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        s = _db->Get(ReadOptions(), handle, key, value);
    }
    StarRocksMetrics::instance()->meta_read_request_duration_us.increment(duration_ns / 1000);
    return to_status(s);
}

Status KVStore::put(ColumnFamilyIndex column_family_index, const std::string& key, const std::string& value) {
    StarRocksMetrics::instance()->meta_write_request_total.increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    int64_t duration_ns = 0;
    rocksdb::Status s;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Put(write_options, handle, key, value);
    }
    StarRocksMetrics::instance()->meta_write_request_duration_us.increment(duration_ns / 1000);
    LOG_IF(WARNING, !s.ok()) << s.ToString();
    return to_status(s);
}

Status KVStore::write_batch(rocksdb::WriteBatch* batch) {
    StarRocksMetrics::instance()->meta_write_request_total.increment(1);
    int64_t duration_ns = 0;
    rocksdb::Status s;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Write(write_options, batch);
    }
    StarRocksMetrics::instance()->meta_write_request_duration_us.increment(duration_ns / 1000);
    LOG_IF(WARNING, !s.ok()) << s.ToString();
    return to_status(s);
}

Status KVStore::remove(ColumnFamilyIndex column_family_index, const std::string& key) {
    StarRocksMetrics::instance()->meta_write_request_total.increment(1);
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    rocksdb::Status s;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        WriteOptions write_options;
        write_options.sync = config::sync_tablet_meta;
        s = _db->Delete(write_options, handle, key);
    }
    StarRocksMetrics::instance()->meta_write_request_duration_us.increment(duration_ns / 1000);
    LOG_IF(WARNING, !s.ok()) << s.ToString();
    return to_status(s);
}

// get iterate upper bound for prefix range query, for example:
//
//   prefix(ascii):      00001
//   prefix_end(ascii):  00002
//
//   prefix(hex):      00ffff
//   prefix_end(hex):  010000
static std::string get_iterate_upper_bound(const std::string& prefix) {
    std::string ret = prefix;
    for (ssize_t i = (ssize_t)ret.size() - 1; i >= 0; i--) {
        if ((uint8_t)ret[i] < 255) {
            ret[i]++;
            return ret;
        } else {
            ret[i] = 0;
        }
    }
    return {};
}

Status KVStore::iterate(ColumnFamilyIndex column_family_index, const std::string& prefix,
                        std::function<bool(std::string_view, std::string_view)> const& func, int64_t timeout_sec) {
    int64_t t_start = MonotonicMillis();
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    auto opts = ReadOptions();
    std::string upper_bound = get_iterate_upper_bound(prefix);
    rocksdb::Slice upper_bound_slice;
    if (!upper_bound.empty()) {
        upper_bound_slice = rocksdb::Slice(upper_bound);
        opts.iterate_upper_bound = &upper_bound_slice;
    }
    std::unique_ptr<Iterator> it(_db->NewIterator(opts, handle));
    if (prefix.empty()) {
        it->SeekToFirst();
    } else {
        it->Seek(prefix);
    }
    // if limit time is less than or equal to zero, it means no limit
    if (timeout_sec <= 0) {
        for (; it->Valid(); it->Next()) {
            if (!prefix.empty()) {
                if (!it->key().starts_with(prefix)) {
                    return Status::OK();
                }
            }
            std::string_view key(it->key().data(), it->key().size());
            std::string_view value(it->value().data(), it->value().size());
            bool ret = func(key, value);
            if (!ret) {
                break;
            }
        }
    } else {
        for (; it->Valid(); it->Next()) {
            if (!prefix.empty()) {
                if (!it->key().starts_with(prefix)) {
                    return Status::OK();
                }
            }
            std::string_view key(it->key().data(), it->key().size());
            std::string_view value(it->value().data(), it->value().size());
            bool ret = func(key, value);
            if (!ret) {
                break;
            }
            if (MonotonicMillis() - t_start > timeout_sec * 1000) {
                LOG(WARNING) << "rocksdb iterate timeout: " << MonotonicMillis() - t_start
                             << ", limit: " << timeout_sec * 1000;
                return Status::TimedOut("rocksdb iterate timeout");
            }
        }
    }
    LOG_IF(WARNING, !it->status().ok()) << it->status().ToString();
    return to_status(it->status());
}

Status KVStore::iterate_range(ColumnFamilyIndex column_family_index, const std::string& lower_bound,
                              const std::string& upper_bound,
                              std::function<bool(std::string_view, std::string_view)> const& func) {
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    rocksdb::Slice iter_upper(upper_bound);
    ReadOptions options;
    options.iterate_upper_bound = &iter_upper;
    std::unique_ptr<Iterator> it(_db->NewIterator(options, handle));
    it->Seek(lower_bound);
    for (; it->Valid(); it->Next()) {
        std::string_view key(it->key().data(), it->key().size());
        std::string_view value(it->value().data(), it->value().size());
        if (!func(key, value)) {
            break;
        }
    }
    LOG_IF(WARNING, !it->status().ok()) << it->status().ToString();
    return to_status(it->status());
}

Status KVStore::compact() {
    rocksdb::ColumnFamilyHandle* handle = _handles[META_COLUMN_FAMILY_INDEX];
    rocksdb::CompactRangeOptions opts;
    auto st = _db->CompactRange(opts, handle, nullptr, nullptr);
    return to_status(st);
}

Status KVStore::flushWAL() {
    return to_status(_db->FlushWAL(true));
}

Status KVStore::flushMemTable() {
    return to_status(_db->Flush(rocksdb::FlushOptions()));
}

std::string KVStore::get_stats() {
    rocksdb::ColumnFamilyHandle* handle = _handles[META_COLUMN_FAMILY_INDEX];
    std::string stats;
    if (!_db->GetProperty(handle, "rocksdb.stats", &stats)) {
        LOG(WARNING) << "rocksdb get stats failed" << std::endl;
    }
    return stats;
}

bool KVStore::get_live_sst_files_size(uint64_t* live_sst_files_size) {
    rocksdb::ColumnFamilyHandle* handle = _handles[META_COLUMN_FAMILY_INDEX];
    return _db->GetIntProperty(handle, "rocksdb.live-sst-files-size", live_sst_files_size);
}

std::string KVStore::get_root_path() {
    return _root_path;
}

} // namespace starrocks
