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

#include <ostream>

#include "column/chunk.h"
#include "exec/sorting/sort_permute.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/chunk_aggregator.h"
#include "storage/olap_define.h"

namespace starrocks {

class SlotDescriptor;
class TabletSchema;

class MemTableSink;

class MemTable {
public:
    MemTable(int64_t tablet_id, const Schema* schema, const std::vector<SlotDescriptor*>* slot_descs,
             MemTableSink* sink, MemTracker* mem_tracker);

    MemTable(int64_t tablet_id, const Schema* schema, MemTableSink* sink, int64_t max_buffer_size,
             MemTracker* mem_tracker);

    MemTable(int64_t tablet_id, const Schema* schema, const std::vector<SlotDescriptor*>* slot_descs,
             MemTableSink* sink, std::string merge_condition, MemTracker* mem_tracker);

    ~MemTable();

    int64_t tablet_id() const { return _tablet_id; }

    // the total memory used (contain tmp chunk and aggregator chunk)
    size_t memory_usage() const;
    MemTracker* mem_tracker() { return _mem_tracker; }

    // buffer memory usage for write segment
    size_t write_buffer_size() const;
    size_t write_buffer_rows() const;

    // return true suggests caller should flush this memory table
    bool insert(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    Status flush(SegmentPB* seg_info = nullptr);

    Status finalize();

    bool is_full() const;

    void set_write_buffer_row(size_t max_buffer_row) { _max_buffer_row = max_buffer_row; }

    static Schema convert_schema(const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs);

    ChunkPtr get_result_chunk() { return _result_chunk; }

private:
    void _merge();

    void _sort(bool is_final, bool by_sort_key = false);
    void _sort_column_inc(bool by_sort_key = false);
    void _append_to_sorted_chunk(Chunk* src, Chunk* dest, bool is_final);

    void _init_aggregator_if_needed();
    void _aggregate(bool is_final);

    Status _split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts, std::unique_ptr<Column>* deletes);

    ChunkPtr _chunk;
    ChunkPtr _result_chunk;

    // for sort by columns
    SmallPermutation _permutations;
    std::vector<uint32_t> _selective_values;

    int64_t _tablet_id;

    const Schema* _vectorized_schema;
    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;
    KeysType _keys_type;

    MemTableSink* _sink;

    // aggregate
    std::unique_ptr<ChunkAggregator> _aggregator;

    uint64_t _merge_count = 0;

    bool _has_op_slot = false;
    std::unique_ptr<Column> _deletes;

    std::string _merge_condition;

    int64_t _max_buffer_size = config::write_buffer_size;
    // initial value is max size
    size_t _max_buffer_row = std::numeric_limits<size_t>::max();
    size_t _total_rows = 0;
    size_t _merged_rows = 0;

    // memory statistic
    MemTracker* _mem_tracker = nullptr;
    // memory usage and bytes usage calculation cost of object column is high,
    // so cache calculated memory usage and bytes usage to avoid repeated calculation.
    size_t _chunk_memory_usage = 0;
    size_t _chunk_bytes_usage = 0;
    size_t _aggregator_memory_usage = 0;
    size_t _aggregator_bytes_usage = 0;
};

inline std::ostream& operator<<(std::ostream& os, const MemTable& table) {
    os << "MemTable(addr=" << &table << ", tablet=" << table.tablet_id() << ", mem=" << table.memory_usage();
    return os;
}

} // namespace starrocks
