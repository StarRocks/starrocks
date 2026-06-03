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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/options.h

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

#include "io/seekable_input_stream.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/page_handle.h"

namespace starrocks {

class FileSystem;
class RandomAccessFile;

namespace lake {
class IndexDeltaGroupLoader;
} // namespace lake

static const uint32_t DEFAULT_PAGE_SIZE = 1024 * 1024; // default size: 1M

class PageBuilderOptions {
public:
    PageBuilderOptions();

    uint32_t data_page_size = DEFAULT_PAGE_SIZE;
    uint32_t dict_page_size;
};

class IndexReadOptions {
public:
    bool use_page_cache = false;
    // for lake tablet
    LakeIOOptions lake_io_opts{.fill_data_cache = true};

    //RandomAccessFile* read_file = nullptr;
    io::SeekableInputStream* read_file = nullptr;
    OlapReaderStatistics* stats = nullptr;

    std::optional<size_t> segment_rows = std::nullopt;

    // ============================================================
    // Index Delta Group context (lake-only).
    //
    // If an ADD INDEX fast-path alter has produced a .idx file that covers
    // this (col_unique_id, index_type), readers prefer it over the segment
    // footer-embedded index. `idg_loader` resolves segment_id -> IDG
    // entries; `tablet_id`/`segment_id` identify this segment; `query_version`
    // drives snapshot visibility (entries with version > query_version are
    // hidden so older snapshots see the pre-alter index or no index).
    // `col_unique_id` disambiguates per-column lookups.
    //
    // Populated by SegmentIterator::_index_read_options from the enclosing
    // SegmentReadOptions. Nullptr `idg_loader` keeps the reader on the
    // traditional footer-embedded path, which is the existing behavior.
    // ============================================================
    std::shared_ptr<lake::IndexDeltaGroupLoader> idg_loader;
    uint64_t tablet_id = 0;
    uint32_t segment_id = 0;
    int64_t query_version = 0;
    int32_t col_unique_id = -1;
};

} // namespace starrocks
