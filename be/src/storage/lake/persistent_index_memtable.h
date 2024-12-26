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

#include "storage/persistent_index.h"
#include "util/phmap/btree.h"

namespace starrocks::lake {

using KeyIndex = size_t;
using KeyIndexSet = std::set<KeyIndex>;

class PersistentIndexMemtable {
public:
    PersistentIndexMemtable(uint64_t max_rss_rowid = 0) : _max_rss_rowid(max_rss_rowid) {}
    // |version|: version of index values
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                  KeyIndexSet* not_founds, size_t* num_found, int64_t version);

    // |version|: version of index values
    Status insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version);

    // |version|: version of index values
    // |rowset_id|: The rowset that keys belong to. Used for setup rebuild point
    Status erase(size_t n, const Slice* keys, IndexValue* old_values, KeyIndexSet* not_founds, size_t* num_found,
                 int64_t version, uint32_t rowset_id);

    // Erase from index, used when rebuild index.
    // |n| : key count
    // |keys| : key array as raw buffer
    // |filter| : used for filter keys that need to skip. `True` means need skip.
    // |version|: version of index values
    // |rowset_id|: The rowset that keys belong to. Used for setup rebuild point
    Status erase_with_filter(size_t n, const Slice* keys, const std::vector<bool>& filter, int64_t version,
                             uint32_t rowset_id);

    // |version|: version of index values
    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes,
                   int64_t version);

    // |version|: version of index values
    Status get(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* not_founds, int64_t version) const;

    // batch get
    // |keys|: key array as raw buffer
    // |values|: value array
    // |key_indexes|: the indexes of keys to be found.
    // |found_key_indexes|: return the found indexes of keys.
    // |version|: version of values
    Status get(const Slice* keys, IndexValue* values, const KeyIndexSet& key_indexes, KeyIndexSet* found_key_indexes,
               int64_t version) const;

    size_t memory_usage() const;

    Status flush(WritableFile* wf, uint64_t* filesize);

    void clear();

    const uint64_t max_rss_rowid() const { return _max_rss_rowid; }

private:
    static void update_index_value(IndexValueWithVer* index_value_info, int64_t version, const IndexValue& value);

private:
    // The size can be up to 230K. The performance of std::map may be poor.
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> _map;
    int64_t _keys_size{0};
    uint64_t _max_rss_rowid{0};
};

} // namespace starrocks::lake
