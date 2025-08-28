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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/zone_map_index.h

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
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/rowset/binary_plain_page.h"
#include "util/once.h"
#include "util/slice.h"

namespace starrocks {

class FileSystem;
class WritableFile;

// Zone map index is represented by an IndexedColumn with ordinal index.
// The IndexedColumn stores serialized ZoneMapPB for each data page.
// It also create and store the segment-level zone map in the index meta so that
// reader can prune an entire segment without reading pages.
class ZoneMapIndexWriter {
public:
    static std::unique_ptr<ZoneMapIndexWriter> create(TypeInfo* type_info);

    virtual ~ZoneMapIndexWriter() = default;

    virtual void enable_truncate_string() = 0;

    virtual void add_values(const void* values, size_t count) = 0;

    virtual void add_nulls(uint32_t count) = 0;

    // mark the end of one data page so that we can finalize the corresponding zone map
    virtual Status flush() = 0;

    // Return the zonemap of last page
    virtual std::optional<ZoneMapPB> get_last_zonemap() = 0;

    virtual Status finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) = 0;

    virtual uint64_t size() const = 0;
};

class ZoneMapIndexReader {
public:
    ZoneMapIndexReader();
    ~ZoneMapIndexReader();

    // load all page zone maps into memory.
    //
    // Multiple callers may call this method concurrently, but only the first one
    // can load the data, the others will wait until the first one finished loading
    // data.
    //
    // Return true if the index data was successfully loaded by the caller, false if
    // the data was loaded by another caller.
    StatusOr<bool> load(const IndexReadOptions& opts, const ZoneMapIndexPB& meta);

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    const std::vector<ZoneMapPB>& page_zone_maps() const { return _page_zone_maps; }

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    int32_t num_pages() const { return static_cast<int32_t>(_page_zone_maps.size()); }

    bool loaded() const { return invoked(_load_once); }

    size_t mem_usage() const;

private:
    void _reset() { std::vector<ZoneMapPB>{}.swap(_page_zone_maps); }

    Status _do_load(const IndexReadOptions& opts, const ZoneMapIndexPB& meta);

    OnceFlag _load_once;
    std::vector<ZoneMapPB> _page_zone_maps;
};

enum CreateIndexDecision {
    Unknown,
    Good, // It deserves to create the index
    Bad,  // It's a bad index
};

// ZoneMapIndexQualityJudger is used to judge whether to write index for string types based on overlap quality.
//
// Greater overlap implies reduced clustering. Therefore, clustering can be quantified using the overlap ratio of zonemaps.
// To calculate:
// 1. s_k(i): Sum of overlaps with page(i) across all pages.
// 2. overlap_ratio: Total sum of s_k divided by the square of the number of pages: (Σ s_k) / (num_pages²).
// 3. Quality: Defined as 1 - overlap_ratio, where higher values indicate better clustering.
// 4. If pages are perfectly separated: Quality ≈ 1.
// 5. If all pages overlap completely: Quality = 0.
class ZoneMapIndexQualityJudger {
public:
    static std::unique_ptr<ZoneMapIndexQualityJudger> create(TypeInfo* type_info, double overlap_threshold,
                                                             int32_t sample_pages);

    virtual ~ZoneMapIndexQualityJudger() = default;

    // Feed the zonemap into this judger, it will be buffered until it can make a decision
    virtual void feed(const ZoneMapPB& page_zone_map) = 0;

    // Make a decision based on the overlap quality.
    // If the overlap quality is good, return Good.
    // If the overlap quality is bad, return Bad.
    // If the sampled pages are not enough, return Unknown.
    virtual CreateIndexDecision make_decision() const = 0;
};

} // namespace starrocks
