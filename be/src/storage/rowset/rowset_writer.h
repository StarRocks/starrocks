// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_writer.h

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

#include "common/statusor.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/types.pb.h"
#include "gutil/macros.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/column_mapping.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer_context.h"

namespace butil {
class IOBuf;
}

namespace starrocks {

namespace vectorized {
class Chunk;
class Column;
} // namespace vectorized

// RowsetWriter is responsible for writing data into segment by row or chunk.
// Only BetaRowsetWriter supports chunk.
// Usage Example:
//      // create writer
//      std::unique_ptr<RowsetWriter> writer;
//      RowsetFactory::create_rowset_writer(writer_context, &writer);
//
//      // write data
//      // 1. serial add chunk
//      // should ensure the order of data between chunks
//      // flush segment when size or number of rows reaches certain condition
//      writer->add_chunk(chunk1);
//      writer->add_chunk(chunk2);
//      ...
//      writer->flush();
//
//      // 2. parallel add chunk
//      // each chunk generates a segment
//      writer->flush_chunk(chunk);
//
//      // 3. add chunk by columns
//      for (column_group : column_groups) {
//          writer->add_columns(chunk1, column_group, is_key);
//          writer->add_columns(chunk2, column_group, is_key);
//          ...
//          writer->flush_columns();
//      }
//      writer->final_flush();
//
//      // finish
//      writer->build();
//
class RowsetWriter {
public:
    RowsetWriter() = default;
    virtual ~RowsetWriter() = default;

    RowsetWriter(const RowsetWriter&) = delete;
    const RowsetWriter& operator=(const RowsetWriter&) = delete;

    virtual Status init() = 0;

    virtual Status add_chunk(const vectorized::Chunk& chunk) { return Status::NotSupported("RowsetWriter::add_chunk"); }

    // Used for vertical compaction
    // |Chunk| contains partial columns data corresponding to |column_indexes|.
    virtual Status add_columns(const vectorized::Chunk& chunk, const std::vector<uint32_t>& column_indexes,
                               bool is_key) {
        return Status::NotSupported("RowsetWriter::add_columns");
    }

    virtual Status flush_chunk(const vectorized::Chunk& chunk, SegmentPB* seg_info = nullptr) {
        return Status::NotSupported("RowsetWriter::flush_chunk");
    }

    virtual Status flush_chunk_with_deletes(const vectorized::Chunk& upserts, const vectorized::Column& deletes,
                                            SegmentPB* seg_info = nullptr) {
        return Status::NotSupported("RowsetWriter::flush_chunk_with_deletes");
    }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset(RowsetSharedPtr rowset) { return Status::NotSupported("RowsetWriter::add_rowset"); }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) {
        return Status::NotSupported("RowsetWriter::add_rowset_for_linked_schema_change");
    }

    // explicit flush all buffered rows into segment file.
    virtual Status flush() { return Status::NotSupported("RowsetWriter::flush"); }

    // Used for vertical compaction
    // flush columns data and index
    virtual Status flush_columns() { return Status::NotSupported("RowsetWriter::flush_columns"); }

    // flush segments footer
    virtual Status final_flush() { return Status::NotSupported("RowsetWriter::final_flush"); }

    // finish building and return pointer to the built rowset (guaranteed to be inited).
    // return nullptr when failed
    virtual StatusOr<RowsetSharedPtr> build() = 0;

    virtual Status flush_segment(const SegmentPB& segment_pb, butil::IOBuf& data) {
        return Status::NotSupported("RowsetWriter::flush_segment");
    }

    virtual Version version() = 0;

    virtual int64_t num_rows() = 0;

    virtual int64_t total_data_size() = 0;

    virtual RowsetId rowset_id() = 0;

    virtual const vectorized::DictColumnsValidMap& global_dict_columns_valid_info() const = 0;
};

} // namespace starrocks
