// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/merger.cpp

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

#include "storage/merger.h"

#include <memory>
#include <vector>

#include "storage/olap_define.h"
#include "storage/reader.h"
#include "storage/row_cursor.h"
#include "storage/tablet.h"
#include "util/trace.h"

namespace starrocks {

OLAPStatus Merger::merge_rowsets(int64_t mem_limit, const TabletSharedPtr& tablet, ReaderType reader_type,
                                 const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                 RowsetWriter* dst_rowset_writer, Merger::Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");

    Reader reader;
    ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = reader_type;
    reader_params.rs_readers = src_rowset_readers;
    reader_params.version = dst_rowset_writer->version();
    int64_t num_rows = 0;
    int64_t total_row_size = 0;
    uint64_t chunk_size = DEFAULT_CHUNK_SIZE;
    if (mem_limit > 0) {
        for (auto& rowset_reader : src_rowset_readers) {
            num_rows += rowset_reader->rowset()->num_rows();
            total_row_size += rowset_reader->rowset()->total_row_size();
        }

        int64_t avg_row_size = (total_row_size + 1) / (num_rows + 1);
        // The result of thie division operation be zero, so added one
        chunk_size = 1 + mem_limit / (src_rowset_readers.size() * avg_row_size + 1);
    }
    if (chunk_size > config::vector_chunk_size) {
        chunk_size = config::vector_chunk_size;
    }
    reader_params.chunk_size = chunk_size;
    RETURN_NOT_OK(reader.init(reader_params));

    std::unique_ptr<MemPool> mem_pool(new MemPool());

    RowCursor row_cursor;
    RETURN_NOT_OK_LOG(row_cursor.init(tablet->tablet_schema()),
                      "failed to init row cursor when merging rowsets of tablet " + tablet->full_name());
    RETURN_NOT_OK_LOG(row_cursor.allocate_memory_for_string_type(tablet->tablet_schema()),
                      "failed to allocate memory for compaction");

    // The following procedure would last for long time, half of one day, etc.
    int64_t output_rows = 0;
    while (true) {
        ObjectPool objectPool;
        bool eof = false;
        // Read one row into row_cursor
        RETURN_NOT_OK_LOG(reader.next_row_with_aggregation(&row_cursor, mem_pool.get(), &objectPool, &eof),
                          "failed to read next row when merging rowsets of tablet " + tablet->full_name());
        if (eof) {
            break;
        }
        RETURN_NOT_OK_LOG(dst_rowset_writer->add_row(row_cursor),
                          "failed to write row when merging rowsets of tablet " + tablet->full_name());
        output_rows++;
        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }

    RETURN_NOT_OK_LOG(dst_rowset_writer->flush(),
                      "failed to flush rowset when merging rowsets of tablet " + tablet->full_name());
    return OLAP_SUCCESS;
}

} // namespace starrocks
