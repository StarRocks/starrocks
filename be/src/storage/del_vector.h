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

#include <roaring/roaring.hh>

#include "common/status.h"
#include "storage/olap_common.h"

namespace starrocks {

using Roaring = roaring::Roaring;

// A bitmap(uint32_t set) to store all the deleted rows' ids of a segment.
// Each DelVector is associated with a version, which is EditVersion's majar version.
// Serialization format:
// |<format version (currently 0x01)> 1 byte|serialized roaring bitmap|
class DelVector {
public:
    DelVector();
    ~DelVector();

    DelVector(DelVector&&) = default;
    DelVector& operator=(DelVector&&) = default;

    int64_t version() const { return _version; }

    void set_empty();

    // create a new DelVector based on this delvec and add more deleted ids
    void add_dels_as_new_version(const std::vector<uint32_t>& dels, int64_t version,
                                 std::shared_ptr<DelVector>* pdelvec) const;

    size_t cardinality() const { return _cardinality; }

    size_t memory_usage() const { return _memory_usage; }

    Status load(int64_t version, const char* data, size_t length);

    void init(int64_t version, const uint32_t* data, size_t length);

    std::string save() const;

    void save_to(std::string* str) const;

    std::string to_string() const;

    bool empty() const { return !_roaring; }

    Roaring* roaring() { return _roaring.get(); }

    void copy_from(const DelVector& delvec);

private:
    void _add_dels(const std::vector<uint32_t>& dels);

    void _update_stats();

    bool _loaded = false;
    int64_t _version = 1;
    size_t _cardinality = 0;
    size_t _memory_usage = 0;
    std::unique_ptr<Roaring> _roaring;
};

typedef std::shared_ptr<DelVector> DelVectorPtr;

class DelvecLoader {
public:
    DelvecLoader() = default;
    virtual ~DelvecLoader() = default;
    virtual Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) = 0;
};

} // namespace starrocks
