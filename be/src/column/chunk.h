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

#include <runtime/types.h>

#include <string_view>

#include "butil/containers/flat_map.h"
#include "column/column.h"
#include "column/column_hash.h"
#include "column/schema.h"
#include "common/global_types.h"
#include "exec/query_cache/owner_info.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class ChunkPB;

class DatumTuple;
class ChunkExtraData;
using ChunkExtraDataPtr = std::shared_ptr<ChunkExtraData>;

/**
 * ChunkExtraData is an extra data which can be used to extend Chunk and 
 * attach extra infos beside the schema. eg, In Stream MV scenes, 
 * the hidden `_op_` column can be added in the ChunkExtraData.
 *
 * NOTE: Chunk only offers the set/get methods for ChunkExtraData, extra data
 * callers need implement the Chunk's specific methods , eg, handle `filter` method 
 * for the extra data.
 */
class ChunkExtraData {
public:
    ChunkExtraData() = default;
    virtual ~ChunkExtraData() = default;
};

class Chunk {
public:
    enum RESERVED_COLUMN_SLOT_ID {
        HASH_JOIN_SPILL_HASH_SLOT_ID = -1,
        SORT_ORDINAL_COLUMN_SLOT_ID = -2,
        HASH_JOIN_BUILD_INDEX_SLOT_ID = -3,
        HASH_JOIN_PROBE_INDEX_SLOT_ID = -4
    };

    using ChunkPtr = std::shared_ptr<Chunk>;
    using SlotHashMap = phmap::flat_hash_map<SlotId, size_t, StdHash<SlotId>>;
    using ColumnIdHashMap = phmap::flat_hash_map<ColumnId, size_t, StdHash<SlotId>>;

    Chunk();
    Chunk(Columns columns, SchemaPtr schema);
    Chunk(Columns columns, SlotHashMap slot_map);

    // Chunk with extra data implements.
    Chunk(Columns columns, SchemaPtr schema, ChunkExtraDataPtr extra_data);
    Chunk(Columns columns, SlotHashMap slot_map, ChunkExtraDataPtr extra_data);

    Chunk(Chunk&& other) = default;
    Chunk& operator=(Chunk&& other) = default;

    ~Chunk() = default;

    // Disallow copy and assignment.
    Chunk(const Chunk& other) = delete;
    Chunk& operator=(const Chunk& other) = delete;

    // Remove all records and reset the delete state.
    void reset();

    Status upgrade_if_overflow();

    Status downgrade();

    bool has_large_column() const;

    bool has_rows() const { return num_rows() > 0; }
    bool is_empty() const { return num_rows() == 0; }
    bool has_columns() const { return !_columns.empty(); }
    size_t num_columns() const { return _columns.size(); }
    size_t num_rows() const { return _columns.empty() ? 0 : _columns[0]->size(); }

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
    std::string_view get_column_name(size_t idx) const;

    // schema must exist and will be updated.
    void append_column(ColumnPtr column, const FieldPtr& field);

    void append_vector_column(ColumnPtr column, const FieldPtr& field, SlotId slot_id);

    void append_column(ColumnPtr column, SlotId slot_id);
    void insert_column(size_t idx, ColumnPtr column, const FieldPtr& field);

    void update_column(ColumnPtr column, SlotId slot_id);
    void update_column_by_index(ColumnPtr column, size_t idx);

    void append_or_update_column(ColumnPtr column, SlotId slot_id);

    void update_rows(const Chunk& src, const uint32_t* indexes);

    void append_default();

    void remove_column_by_index(size_t idx);
    void remove_column_by_slot_id(SlotId slot_id);

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

    // Must ensure the slot_id exist
    const ColumnPtr& get_column_by_slot_id(SlotId slot_id) const;
    ColumnPtr& get_column_by_slot_id(SlotId slot_id);

    bool is_column_nullable(SlotId slot_id) const;

    void set_slot_id_to_index(SlotId slot_id, size_t idx) { _slot_id_to_index[slot_id] = idx; }
    bool is_slot_exist(SlotId id) const { return _slot_id_to_index.contains(id); }
    void reset_slot_id_to_index() { _slot_id_to_index.clear(); }
    size_t get_index_by_slot_id(SlotId slot_id) { return _slot_id_to_index[slot_id]; }

    void set_columns(const Columns& columns) { _columns = columns; }

    // Create an empty chunk with the same meta and reserve it of size chunk _num_rows
    ChunkUniquePtr clone_empty() const;
    ChunkUniquePtr clone_empty_with_slot() const;
    ChunkUniquePtr clone_empty_with_schema() const;
    // Create an empty chunk with the same meta and reserve it of specified size.
    ChunkUniquePtr clone_empty(size_t size) const;
    ChunkUniquePtr clone_empty_with_slot(size_t size) const;
    ChunkUniquePtr clone_empty_with_schema(size_t size) const;
    ChunkUniquePtr clone_unique() const;

    void append(const Chunk& src) { append(src, 0, src.num_rows()); }
    void merge(Chunk&& src);

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

    // This function will append data from src according to the input indexes.
    // The columns of src chunk will be destroyed after append。
    // Peak memory usage can be reduced when src chunk has a large number of rows and columns
    void rolling_append_selective(Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size);

    // Remove rows from this chunk according to the vector |selection|.
    // The n-th row will be removed if selection[n] is zero.
    // The size of |selection| must be equal to the number of rows.
    // @param force whether check zero-count of filter, skip the filter procedure if no data to filter
    // @return the number of rows after filter.
    size_t filter(const Buffer<uint8_t>& selection, bool force = false);

    // Return the number of rows after filter.
    size_t filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to);

    // Return the data of n-th row.
    // This method is relatively slow and mainly used for unit tests now.
    DatumTuple get(size_t n) const;

    void set_delete_state(DelCondSatisfied state) { _delete_state = state; }

    DelCondSatisfied delete_state() const { return _delete_state; }

    const SlotHashMap& get_slot_id_to_index_map() const { return _slot_id_to_index; }

    // Call `Column::reserve` on each column of |chunk|, with |cap| passed as argument.
    void reserve(size_t cap);

    // Chunk memory usage, used for memory limit.
    // Including container capacity size and element data size.
    // 1. object column: (column container capacity * type size) + pointer element serialize data size
    // 2. other columns: column container capacity * type size
    size_t memory_usage() const;

    // Column container memory usage
    size_t container_memory_usage() const;
    // Element memory usage that is not in the container, such as memory referenced by pointer.
    size_t reference_memory_usage() const { return reference_memory_usage(0, num_rows()); }
    // Element memory usage of |size| rows from |from| in chunk
    size_t reference_memory_usage(size_t from, size_t size) const;

    // Chunk bytes usage, used for memtable data size statistic.
    // Including element data size only.
    // 1. object column: serialize data size
    // 2. other columns: column container size * type size
    size_t bytes_usage() const;
    // Bytes usage of |size| rows from |from| in chunk
    size_t bytes_usage(size_t from, size_t size) const;

    bool has_const_column() const;

    void materialized_nullable() {
        for (ColumnPtr& c : _columns) {
            c->materialized_nullable();
        }
    }

    // Unpack and duplicate const columns in the chunk.
    void unpack_and_duplicate_const_columns();

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

    std::string debug_row(size_t index) const;

    std::string debug_columns() const;

    std::string rebuild_csv_row(size_t index, const std::string& delimiter) const;

    Status capacity_limit_reached() const {
        for (const auto& column : _columns) {
            RETURN_IF_ERROR(column->capacity_limit_reached());
        }
        return Status::OK();
    }

    query_cache::owner_info& owner_info() { return _owner_info; }
    const ChunkExtraDataPtr& get_extra_data() const { return _extra_data; }
    void set_extra_data(ChunkExtraDataPtr data) { this->_extra_data = std::move(data); }
    bool has_extra_data() const { return this->_extra_data != nullptr; }

private:
    void rebuild_cid_index();

    Columns _columns;
    std::shared_ptr<Schema> _schema;
    ColumnIdHashMap _cid_to_index;
    // For compatibility
    SlotHashMap _slot_id_to_index;
    DelCondSatisfied _delete_state = DEL_NOT_SATISFIED;
    query_cache::owner_info _owner_info;
    ChunkExtraDataPtr _extra_data;
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
    DCHECK(is_slot_exist(slot_id)) << slot_id;
    size_t idx = _slot_id_to_index[slot_id];
    return _columns[idx];
}

inline bool Chunk::is_column_nullable(SlotId slot_id) const {
    return get_column_by_slot_id(slot_id)->is_nullable();
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

} // namespace starrocks
