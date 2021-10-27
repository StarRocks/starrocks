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

#ifndef STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_H
#define STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_H

#include "gen_cpp/types.pb.h"
#include "gutil/macros.h"
#include "runtime/global_dicts.h"
#include "storage/column_mapping.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer_context.h"

namespace starrocks {

struct ContiguousRow;
class RowCursor;

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
//      // 1. serial add row
//      // should ensure the order of data between rows
//      // flush segment when size or number of rows reaches certain condition
//      writer->add_row(row1);
//      writer->add_row(row2);
//      ...
//      writer->flush();
//
//      // 2. serial add chunk
//      // should ensure the order of data between chunks
//      // flush segment when size or number of rows reaches certain condition
//      writer->add_chunk(chunk1);
//      writer->add_chunk(chunk2);
//      ...
//      writer->flush();
//
//      // 3. parallel add chunk
//      // each chunk generates a segment
//      writer->flush_chunk(chunk);
//
//      // finish
//      writer->build();
//
class RowsetWriter {
public:
    RowsetWriter() = default;
    virtual ~RowsetWriter() = default;

    virtual OLAPStatus init() = 0;

    // Memory note: input `row` is guaranteed to be copied into writer's internal buffer, including all slice data
    // referenced by `row`. That means callers are free to de-allocate memory for `row` after this method returns.
    virtual OLAPStatus add_row(const RowCursor& row) = 0;
    virtual OLAPStatus add_row(const ContiguousRow& row) = 0;

    virtual OLAPStatus add_chunk(const vectorized::Chunk& chunk) = 0;

    // Used for updatable tablet compaction (BetaRowsetWriter), need to write src rssid with segment
    virtual OLAPStatus add_chunk_with_rssid(const vectorized::Chunk& chunk, const vector<uint32_t>& rssid) {
        return OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    // This routine is free to modify the content of |chunk|.
    virtual OLAPStatus flush_chunk(const vectorized::Chunk& chunk) = 0;

    virtual OLAPStatus flush_chunk_with_deletes(const vectorized::Chunk& upserts,
                                                const vectorized::Column& deletes) = 0;

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual OLAPStatus add_rowset(RowsetSharedPtr rowset) = 0;

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual OLAPStatus add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                           const SchemaMapping& schema_mapping) = 0;

    // explicit flush all buffered rows into segment file.
    // note that `add_row` could also trigger flush when certain conditions are met
    virtual OLAPStatus flush() = 0;

    // finish building and return pointer to the built rowset (guaranteed to be inited).
    // return nullptr when failed
    virtual RowsetSharedPtr build() = 0;

    virtual Version version() = 0;

    virtual int64_t num_rows() = 0;

    virtual int64_t total_data_size() = 0;

    virtual RowsetId rowset_id() = 0;

    virtual const vectorized::DictColumnsValidMap& global_dict_columns_valid_info() {
        return _global_dict_columns_valid_info;
    }

protected:
    vectorized::DictColumnsValidMap _global_dict_columns_valid_info;

private:
    RowsetWriter(const RowsetWriter&) = delete;
    const RowsetWriter& operator=(const RowsetWriter&) = delete;
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_ROWSET_ROWSET_WRITER_H
