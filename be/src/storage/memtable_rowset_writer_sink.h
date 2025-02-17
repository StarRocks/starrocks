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

#include "gen_cpp/data.pb.h"
#include "gutil/macros.h"
#include "storage/memtable_sink.h"
#include "storage/rowset/rowset_writer.h"

namespace starrocks {

class MemTableRowsetWriterSink : public MemTableSink {
public:
    explicit MemTableRowsetWriterSink(RowsetWriter* w) : _rowset_writer(w) {}
    ~MemTableRowsetWriterSink() override = default;

    DISALLOW_COPY(MemTableRowsetWriterSink);

    Status flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr, bool eos = false,
                       int64_t* flush_data_size = nullptr) override {
        return _rowset_writer->flush_chunk(chunk, seg_info);
    }

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes, SegmentPB* seg_info = nullptr,
                                    bool eos = false, int64_t* flush_data_size = nullptr) override {
        return _rowset_writer->flush_chunk_with_deletes(upserts, deletes, seg_info);
    }

private:
    RowsetWriter* _rowset_writer;
};

} // namespace starrocks
