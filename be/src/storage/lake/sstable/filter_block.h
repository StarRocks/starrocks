// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "util/slice.h"

namespace starrocks {

namespace lake {

namespace sstable {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
public:
    explicit FilterBlockBuilder(const FilterPolicy*);

    FilterBlockBuilder(const FilterBlockBuilder&) = delete;
    FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

    void StartBlock(uint64_t block_offset);
    void AddKey(const Slice& key);
    Slice Finish();

private:
    void GenerateFilter();

    const FilterPolicy* policy_;
    std::string keys_;            // Flattened key contents
    std::vector<size_t> start_;   // Starting index in keys_ of each key
    std::string result_;          // Filter data computed so far
    std::vector<Slice> tmp_keys_; // policy_->CreateFilter() argument
    std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
public:
    // REQUIRES: "contents" and *policy must stay live while *this is live.
    FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
    bool KeyMayMatch(uint64_t block_offset, const Slice& key);

private:
    const FilterPolicy* policy_;
    const char* data_;   // Pointer to filter data (at block-start)
    const char* offset_; // Pointer to beginning of offset array (at block-end)
    size_t num_;         // Number of entries in offset array
    size_t base_lg_;     // Encoding parameter (see kFilterBaseLg in .cc file)
};

} // namespace sstable
} // namespace lake
} // namespace starrocks