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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bloom_filter_index_writer.h

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

#include <cstddef>
#include <memory>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/macros.h"

namespace starrocks {

class TypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

struct BloomFilterOptions;
class WritableFile;

class BloomFilterIndexWriter {
public:
    static Status create(const BloomFilterOptions& bf_options, const TypeInfoPtr& typeinfo,
                         std::unique_ptr<BloomFilterIndexWriter>* res);

    BloomFilterIndexWriter() = default;
    virtual ~BloomFilterIndexWriter() = default;

    virtual void add_values(const void* values, size_t count) = 0;

    virtual void add_nulls(uint32_t count) = 0;

    virtual Status flush() = 0;

    virtual Status finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) = 0;

    virtual uint64_t size() = 0;

private:
    BloomFilterIndexWriter(const BloomFilterIndexWriter&) = delete;
    const BloomFilterIndexWriter& operator=(const BloomFilterIndexWriter&) = delete;
};

} // namespace starrocks
