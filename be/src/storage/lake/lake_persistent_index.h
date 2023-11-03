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

#pragma once

#include "storage/lake/tablet.h"
#include "storage/persistent_index.h"

namespace starrocks::lake {

class PersistentIndexMemtable;
class PersistentIndexSStablePB;
class Tablet;
struct SstableInfo;

class LakePersistentIndex : public PersistentIndex {
public:
    explicit LakePersistentIndex(std::string path);

    LakePersistentIndex(Tablet* tablet);

    ~LakePersistentIndex() override;

    DISALLOW_COPY(LakePersistentIndex);

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    Status get(size_t n, const Slice* keys, IndexValue* values) override;

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |stat|: used for collect statistic
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                  IOStat* stat = nullptr) override;

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1|: also check l1 for insertion consistency(key must not exist previously), may imply heavy IO costs
    Status insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1) override;

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    Status erase(size_t n, const Slice* keys, IndexValue* old_values) override;

    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |max_src_rssid|: maximum of rssid array
    // |failed|: return not match rowid
    Status try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed) override;

    Status minor_compact();

    Status major_compact(int64_t min_retain_version);

    void commit(PersistentIndexSStablePB* pindex_sstable);

    void update_version(int64_t version) { _version = version; }

    void set_txn_id(int64_t txn_id) { _txn_id = txn_id; }

private:
    void flush_to_immutable_memtable();

    bool is_memtable_full();

private:
    std::unique_ptr<PersistentIndexMemtable> _memtable;
    std::unique_ptr<PersistentIndexMemtable> _immutable_memtable{nullptr};
    std::vector<SstableInfo> _sstables;
    Tablet* _tablet{nullptr};
    int64_t _version{0};
    int64_t _txn_id{0};
};

} // namespace starrocks::lake
