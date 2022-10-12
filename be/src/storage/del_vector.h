// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <roaring/roaring.hh>

#include "common/status.h"
#include "storage/olap_common.h"

namespace starrocks {

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

    std::string to_string() const;

    bool empty() const { return !_roaring; }

    Roaring* roaring() { return _roaring.get(); }

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

} // namespace starrocks
