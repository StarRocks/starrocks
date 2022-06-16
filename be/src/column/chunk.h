// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <runtime/types.h>

#include "butil/containers/flat_map.h"
#include "column/column.h"
#include "column/column_hash.h"
#include "column/schema.h"
#include "common/global_types.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class ChunkPB;
namespace vectorized {

class DatumTuple;

class Chunk {
public:
    using ChunkPtr = std::shared_ptr<Chunk>;
    using SlotHashMap = phmap::flat_hash_map<SlotId, size_t, StdHash<SlotId>>;
    using ColumnIdHashMap = phmap::flat_hash_map<ColumnId, size_t, StdHash<SlotId>>;
    using TupleHashMap = phmap::flat_hash_map<TupleId, size_t, StdHash<TupleId>>;

    Chunk();
    Chunk(Columns columns, SchemaPtr schema);
    Chunk(Columns columns, const SlotHashMap& slot_map);
    Chunk(Columns columns, const SlotHashMap& slot_map, const TupleHashMap& tuple_map);

    Chunk(Chunk&& other) = default;
    Chunk& operator=(Chunk&& other) = default;

    ~Chunk() = default;

    // Disallow copy and assignment.
    Chunk(const Chunk& other) = delete;
    Chunk& operator=(const Chunk& other) = delete;

    // Remove all records and reset the delete state.
    void reset();

    bool has_rows() const { return num_rows() > 0; }
    bool is_empty() const { return num_rows() == 0; }
    bool has_columns() const { return !_columns.empty(); }
    bool has_tuple_columns() const { return !_tuple_id_to_index.empty(); }
    size_t num_columns() const { return _columns.size(); }
    size_t num_rows() const { return _columns.empty() ? 0 : _columns[0]->size(); }
    size_t num_tuple_columns() const { return _tuple_id_to_index.size(); }

    // Resize the chunk to contain |count| rows elements.
    //  - If the current size is less than count, additional default values are appended.
    //  - If the current size is greater than count, the chunk is reduced to its first count elements.
    void set_num_rows(size_t count);

    void swap_chunk(Chunk& other);

    const SchemaPtr& schema() const { return _schema; }
    SchemaPtr& schema() { return _schema; }

    const Columns& columns() const { return _columns; }
    Columns& columns() { return _columns; }

    // schema must exists.
    std::string get_column_name(size_t idx) const;

    // schema must exist and will be updated.
    void append_column(ColumnPtr column, const FieldPtr& field);

    void append_column(ColumnPtr column, SlotId slot_id);
    void insert_column(size_t idx, ColumnPtr column, const FieldPtr& field);

    void update_column(ColumnPtr column, SlotId slot_id);

    void append_tuple_column(const ColumnPtr& column, TupleId tuple_id);

    void remove_column_by_index(size_t idx);

    // Remove multiple columns by their indexes.
    // For simplicity and better performance, we are assuming |indexes| all all valid
    // and is sorted in ascending order, if it's not, unexpected columns may be removed (silently).
    // |indexes| can be empty and no column will be removed in this case.
    void remove_columns_by_index(const std::vector<size_t>& indexes);

    // schema must exists.
    const ColumnPtr& get_column_by_name(const std::string& column_name) const;
    ColumnPtr& get_column_by_name(const std::string& column_name);

    const ColumnPtr& get_column_by_index(size_t idx) const;
    ColumnPtr& get_column_by_index(size_t idx);

    const ColumnPtr& get_column_by_id(ColumnId cid) const;
    ColumnPtr& get_column_by_id(ColumnId cid);

    const ColumnPtr& get_tuple_column_by_id(TupleId tuple_id) const;
    ColumnPtr& get_tuple_column_by_id(TupleId tuple_id);

    // Must ensure the slot_id exist
    const ColumnPtr& get_column_by_slot_id(SlotId slot_id) const;
    ColumnPtr& get_column_by_slot_id(SlotId slot_id);

    void set_slot_id_to_index(SlotId slot_id, size_t idx) { _slot_id_to_index[slot_id] = idx; }
    bool is_slot_exist(SlotId id) const { return _slot_id_to_index.contains(id); }
    bool is_tuple_exist(TupleId id) const { return _tuple_id_to_index.contains(id); }
    void reset_slot_id_to_index() { _slot_id_to_index.clear(); }

    void set_columns(const Columns& columns) { _columns = columns; }

    // The size for serialize chunk meta and chunk data
    size_t serialize_size() const;

    // Serialize chunk data and meta to ChunkPB
    // The result value is the chunk data serialize size
    size_t serialize_with_meta(starrocks::ChunkPB* chunk) const;

    // Only serialize chunk data to dst
    // The serialize format:
    //     version(4 byte)
    //     num_rows(4 byte)
    //     column 1 data
    //     column 2 data
    //     ...
    //     column n data
    // Note: You should ensure the dst buffer size is enough
    size_t serialize(uint8_t* dst) const;

    // Deserialize chunk by |src| (chunk data) and |meta| (chunk meta)
    Status deserialize(const uint8_t* src, size_t len, const RuntimeChunkMeta& meta, size_t serialized_size);

    // Create an empty chunk with the same meta and reserve it of size chunk _num_rows
    // not clone tuple column
    std::unique_ptr<Chunk> clone_empty() const;
    std::unique_ptr<Chunk> clone_empty_with_slot() const;
    std::unique_ptr<Chunk> clone_empty_with_schema() const;
    // Create an empty chunk with the same meta and reserve it of specified size.
    // not clone tuple column
    std::unique_ptr<Chunk> clone_empty(size_t size) const;
    std::unique_ptr<Chunk> clone_empty_with_slot(size_t size) const;
    std::unique_ptr<Chunk> clone_empty_with_schema(size_t size) const;
    // Create an empty chunk with the same meta and reserve it of size chunk _num_rows
    std::unique_ptr<Chunk> clone_empty_with_tuple() const;
    // Create an empty chunk with the same meta and reserve it of specified size.
    std::unique_ptr<Chunk> clone_empty_with_tuple(size_t size) const;

    void append(const Chunk& src) { append(src, 0, src.num_rows()); }

    // Append |count| rows from |src|, started from |offset|, to the |this| chunk.
    void append(const Chunk& src, size_t offset, size_t count);

    // columns in chunk may have same column ptr
    // append_safe will check size of all columns in dest chunk
    // to ensure same column will not apppend repeatedly
    void append_safe(const Chunk& src) { append_safe(src, 0, src.num_rows()); }

    // columns in chunk may have same column ptr
    // append_safe will check size of all columns in dest chunk
    // to ensure same column will not apppend repeatedly
    void append_safe(const Chunk& src, size_t offset, size_t count);

    // This function will append data from src according to the input indexes. 'indexes' contains
    // the row index of the src.
    // This function will get row index from indexes and append the data to this chunk.
    // This function will handle indexes start from input 'from' and will append 'size' times
    // For example:
    //      input indexes: [5, 4, 3, 2, 1]
    //      from: 2
    //      size: 2
    // This function will copy the [3, 2] row of src to this chunk.
    void append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size);

    // Remove rows from this chunk according to the vector |selection|.
    // The n-th row will be removed if selection[n] is zero.
    // The size of |selection| must be equal to the number of rows.
    // Return the number of rows after filter.
    size_t filter(const Buffer<uint8_t>& selection);

    // Return the number of rows after filter.
    size_t filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to);

    // Return the data of n-th row.
    // This method is relatively slow and mainly used for unit tests now.
    DatumTuple get(size_t n) const;

    void set_delete_state(DelCondSatisfied state) { _delete_state = state; }

    DelCondSatisfied delete_state() const { return _delete_state; }

    const TupleHashMap& get_tuple_id_to_index_map() const { return _tuple_id_to_index; }
    const SlotHashMap& get_slot_id_to_index_map() const { return _slot_id_to_index; }

    // Call `Column::reserve` on each column of |chunk|, with |cap| passed as argument.
    void reserve(size_t cap);

    // Chunk memory usage, used for memory limit.
    // Including container capacity size and element data size.
    // 1. object column: (column container capacity * type size) + pointer element serialize data size
    // 2. other columns: column container capacity * type size
    size_t memory_usage() const;

    // memory usage after shrink
    size_t shrink_memory_usage() const;

    // Column container memory usage
    size_t container_memory_usage() const;
    // Element memory usage that is not in the container, such as memory referenced by pointer.
    size_t element_memory_usage() const { return element_memory_usage(0, num_rows()); }
    // Element memory usage of |size| rows from |from| in chunk
    size_t element_memory_usage(size_t from, size_t size) const;

    // Chunk bytes usage, used for memtable data size statistic.
    // Including element data size only.
    // 1. object column: serialize data size
    // 2. other columns: column container size * type size
    size_t bytes_usage() const;
    // Bytes usage of |size| rows from |from| in chunk
    size_t bytes_usage(size_t from, size_t size) const;

    bool has_const_column() const;

#ifndef NDEBUG
    // check whether the internal state is consistent, abort the program if check failed.
    void check_or_die();
#else
    void check_or_die() {}
#endif

#ifndef NDEBUG
#define DCHECK_CHUNK(chunk_ptr)          \
    do {                                 \
        if ((chunk_ptr) != nullptr) {    \
            (chunk_ptr)->check_or_die(); \
        }                                \
    } while (false)
#else
#define DCHECK_CHUNK(chunk_ptr)
#endif

    std::string debug_row(uint32_t index) const;

    bool reach_capacity_limit(std::string* msg = nullptr) const {
        for (const auto& column : _columns) {
            if (column->reach_capacity_limit(msg)) {
                return true;
            }
        }
        return false;
    }

private:
    void rebuild_cid_index();

    Columns _columns;
    std::shared_ptr<Schema> _schema;
    ColumnIdHashMap _cid_to_index;
    // For compatibility
    SlotHashMap _slot_id_to_index;
    TupleHashMap _tuple_id_to_index;
    DelCondSatisfied _delete_state = DEL_NOT_SATISFIED;
};

inline const ColumnPtr& Chunk::get_column_by_name(const std::string& column_name) const {
    return const_cast<Chunk*>(this)->get_column_by_name(column_name);
}

inline ColumnPtr& Chunk::get_column_by_name(const std::string& column_name) {
    size_t idx = _schema->get_field_index_by_name(column_name);
    return _columns[idx];
}

inline const ColumnPtr& Chunk::get_column_by_slot_id(SlotId slot_id) const {
    return const_cast<Chunk*>(this)->get_column_by_slot_id(slot_id);
}

inline ColumnPtr& Chunk::get_column_by_slot_id(SlotId slot_id) {
    DCHECK(is_slot_exist(slot_id));
    size_t idx = _slot_id_to_index[slot_id];
    return _columns[idx];
}

inline const ColumnPtr& Chunk::get_column_by_index(size_t idx) const {
    return const_cast<Chunk*>(this)->get_column_by_index(idx);
}

inline ColumnPtr& Chunk::get_column_by_index(size_t idx) {
    DCHECK_LT(idx, _columns.size());
    return _columns[idx];
}

inline const ColumnPtr& Chunk::get_column_by_id(ColumnId cid) const {
    return const_cast<Chunk*>(this)->get_column_by_id(cid);
}

inline ColumnPtr& Chunk::get_column_by_id(ColumnId cid) {
    DCHECK(!_cid_to_index.empty());
    DCHECK(_cid_to_index.contains(cid));
    return _columns[_cid_to_index[cid]];
}

inline const ColumnPtr& Chunk::get_tuple_column_by_id(TupleId tuple_id) const {
    return const_cast<Chunk*>(this)->get_tuple_column_by_id(tuple_id);
}

inline ColumnPtr& Chunk::get_tuple_column_by_id(TupleId tuple_id) {
    return _columns[_tuple_id_to_index[tuple_id]];
}

// Chunk meta for runtime compute
// Currently Used in DataStreamRecvr to deserialize Chunk
struct RuntimeChunkMeta {
    std::vector<TypeDescriptor> types;
    std::vector<bool> is_nulls;
    std::vector<bool> is_consts;
    Chunk::SlotHashMap slot_id_to_index;
    Chunk::TupleHashMap tuple_id_to_index;
};

} // namespace vectorized
} // namespace starrocks
