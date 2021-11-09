// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <ostream>

#include "column/chunk.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/olap_define.h"
#include "storage/vectorized/chunk_aggregator.h"

namespace starrocks {

class RowsetWriter;
class SlotDescriptor;
class TabletSchema;

namespace vectorized {

class MemTable {
public:
    MemTable(int64_t tablet_id, const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs,
             RowsetWriter* rowset_writer, MemTracker* mem_tracker);

    ~MemTable();
    int64_t tablet_id() const { return _tablet_id; }

    // the total memory used (contain tmp chunk and aggregator chunk)
    size_t memory_usage() const;
    MemTracker* mem_tracker() { return _mem_tracker; }

    // buffer memory usage for write segment
    size_t write_buffer_size() const;

    // return true suggests caller should flush this memory table
    bool insert(Chunk* chunk, const uint32_t* indexes, uint32_t from, uint32_t size);
    OLAPStatus flush();
    Status finalize();

    bool is_full() const;

private:
    void _merge();

    void _sort(bool is_final);
    void _sort_chunk_by_columns();
    void _sort_chunk_by_rows();
    void _append_to_sorted_chunk(Chunk* src, Chunk* dest);

    void _aggregate(bool is_final);

    void _split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts, std::unique_ptr<Column>* deletes);

    friend class SortHelper;

    struct PermutationItem {
        uint32_t index_in_chunk;
        uint32_t permutation_index;
    };
    using Permutation = std::vector<PermutationItem>;

    ChunkPtr _chunk;
    ChunkPtr _result_chunk;
    vector<uint8_t> _result_deletes;

    // for sort by columns
    Permutation _permutations;
    std::vector<uint32_t> _selective_values;
    Schema _vectorized_schema;

    int64_t _tablet_id;
    const TabletSchema* _tablet_schema;
    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;
    KeysType _keys_type;

    RowsetWriter* _rowset_writer;

    // aggregate
    std::unique_ptr<ChunkAggregator> _aggregator;

    uint64_t _merge_count = 0;

    bool _has_op_slot = false;
    std::unique_ptr<Column> _deletes;

    // memory statistic
    MemTracker* _mem_tracker = nullptr;
    // memory usage and bytes usage calculation cost of object column is high,
    // so cache calculated memory usage and bytes usage to avoid repeated calculation.
    size_t _chunk_memory_usage = 0;
    size_t _chunk_bytes_usage = 0;
    size_t _aggregator_memory_usage = 0;
    size_t _aggregator_bytes_usage = 0;
}; // class MemTable

} // namespace vectorized

inline std::ostream& operator<<(std::ostream& os, const vectorized::MemTable& table) {
    os << "MemTable(addr=" << &table << ", tablet=" << table.tablet_id() << ", mem=" << table.memory_usage();
    return os;
}

} // namespace starrocks
