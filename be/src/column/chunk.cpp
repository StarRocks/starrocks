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

#include "column/chunk.h"

#include <utility>

#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "gen_cpp/data.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "simd/simd.h"
#include "util/coding.h"

namespace starrocks {

Chunk::Chunk() {
    _slot_id_to_index.reserve(4);
    _tuple_id_to_index.reserve(1);
}

Status Chunk::upgrade_if_overflow() {
    for (auto& column : _columns) {
        auto ret = column->upgrade_if_overflow();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = ret.value();
        } else {
            continue;
        }
    }
    return Status::OK();
}

Status Chunk::downgrade() {
    for (auto& column : _columns) {
        auto ret = column->downgrade();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = ret.value();
        } else {
            continue;
        }
    }
    return Status::OK();
}

bool Chunk::has_large_column() const {
    for (const auto& column : _columns) {
        if (column->has_large_column()) {
            return true;
        }
    }
    return false;
}

Chunk::Chunk(Columns columns, SchemaPtr schema) : Chunk(std::move(columns), std::move(schema), nullptr) {}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns columns, SlotHashMap slot_map) : Chunk(std::move(columns), std::move(slot_map), nullptr) {}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns columns, SlotHashMap slot_map, TupleHashMap tuple_map)
        : Chunk(std::move(columns), std::move(slot_map), std::move(tuple_map), nullptr) {}

Chunk::Chunk(Columns columns, SchemaPtr schema, ChunkExtraDataPtr extra_data)
        : _columns(std::move(columns)), _schema(std::move(schema)), _extra_data(std::move(extra_data)) {
    // bucket size cannot be 0.
    _cid_to_index.reserve(std::max<size_t>(1, columns.size() * 2));
    _slot_id_to_index.reserve(std::max<size_t>(1, _columns.size() * 2));
    _tuple_id_to_index.reserve(1);
    rebuild_cid_index();
    check_or_die();
}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns columns, SlotHashMap slot_map, ChunkExtraDataPtr extra_data)
        : _columns(std::move(columns)), _slot_id_to_index(std::move(slot_map)), _extra_data(std::move(extra_data)) {
    // when use _slot_id_to_index, we don't need to rebuild_cid_index
    _tuple_id_to_index.reserve(1);
}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns columns, SlotHashMap slot_map, TupleHashMap tuple_map, ChunkExtraDataPtr extra_data)
        : _columns(std::move(columns)),
          _slot_id_to_index(std::move(slot_map)),
          _tuple_id_to_index(std::move(tuple_map)),
          _extra_data(std::move(extra_data)) {
    // when use _slot_id_to_index, we don't need to rebuild_cid_index
}

void Chunk::reset() {
    for (ColumnPtr& c : _columns) {
        c->reset_column();
    }
    _delete_state = DEL_NOT_SATISFIED;
    _extra_data.reset();
}

void Chunk::swap_chunk(Chunk& other) {
    _columns.swap(other._columns);
    _schema.swap(other._schema);
    _cid_to_index.swap(other._cid_to_index);
    _slot_id_to_index.swap(other._slot_id_to_index);
    _tuple_id_to_index.swap(other._tuple_id_to_index);
    std::swap(_delete_state, other._delete_state);
    _extra_data.swap(other._extra_data);
}

void Chunk::set_num_rows(size_t count) {
    for (ColumnPtr& c : _columns) {
        c->resize(count);
    }
}

std::string_view Chunk::get_column_name(size_t idx) const {
    DCHECK_LT(idx, _columns.size());
    return _schema->field(idx)->name();
}

void Chunk::append_column(ColumnPtr column, const FieldPtr& field) {
    DCHECK(!_cid_to_index.contains(field->id()));
    _cid_to_index[field->id()] = _columns.size();
    _columns.emplace_back(std::move(column));
    _schema->append(field);
    check_or_die();
}

void Chunk::append_column(ColumnPtr column, SlotId slot_id) {
    _slot_id_to_index[slot_id] = _columns.size();
    _columns.emplace_back(std::move(column));
    check_or_die();
}

void Chunk::update_column(ColumnPtr column, SlotId slot_id) {
    _columns[_slot_id_to_index[slot_id]] = std::move(column);
    check_or_die();
}

void Chunk::update_column_by_index(ColumnPtr column, size_t idx) {
    _columns[idx] = std::move(column);
    check_or_die();
}

void Chunk::insert_column(size_t idx, ColumnPtr column, const FieldPtr& field) {
    DCHECK_LT(idx, _columns.size());
    _columns.emplace(_columns.begin() + idx, std::move(column));
    _schema->insert(idx, field);
    rebuild_cid_index();
    check_or_die();
}

void Chunk::append_tuple_column(const ColumnPtr& column, TupleId tuple_id) {
    _tuple_id_to_index[tuple_id] = _columns.size();
    _columns.emplace_back(column);
    check_or_die();
}

void Chunk::append_default() {
    for (const auto& column : _columns) {
        column->append_default();
    }
}

void Chunk::remove_column_by_index(size_t idx) {
    DCHECK_LT(idx, _columns.size());
    _columns.erase(_columns.begin() + idx);
    if (_schema != nullptr) {
        _schema->remove(idx);
        rebuild_cid_index();
    }
}

[[maybe_unused]] void Chunk::remove_columns_by_index(const std::vector<size_t>& indexes) {
    DCHECK(std::is_sorted(indexes.begin(), indexes.end()));
    for (size_t i = indexes.size(); i > 0; i--) {
        _columns.erase(_columns.begin() + indexes[i - 1]);
    }
    if (_schema != nullptr && !indexes.empty()) {
        for (size_t i = indexes.size(); i > 0; i--) {
            _schema->remove(indexes[i - 1]);
        }
        rebuild_cid_index();
    }
}

void Chunk::rebuild_cid_index() {
    _cid_to_index.clear();
    for (size_t i = 0; i < _schema->num_fields(); i++) {
        _cid_to_index[_schema->field(i)->id()] = i;
    }
}

std::unique_ptr<Chunk> Chunk::clone_empty() const {
    return clone_empty(num_rows());
}

std::unique_ptr<Chunk> Chunk::clone_empty(size_t size) const {
    if (_columns.size() == _slot_id_to_index.size()) {
        return clone_empty_with_slot(size);
    } else {
        return clone_empty_with_schema(size);
    }
}

std::unique_ptr<Chunk> Chunk::clone_empty_with_slot() const {
    return clone_empty_with_slot(num_rows());
}

std::unique_ptr<Chunk> Chunk::clone_empty_with_slot(size_t size) const {
    DCHECK_EQ(_columns.size(), _slot_id_to_index.size());
    Columns columns(_slot_id_to_index.size());
    for (size_t i = 0; i < _slot_id_to_index.size(); i++) {
        columns[i] = _columns[i]->clone_empty();
        columns[i]->reserve(size);
    }
    return std::make_unique<Chunk>(columns, _slot_id_to_index);
}

std::unique_ptr<Chunk> Chunk::clone_empty_with_schema() const {
    return clone_empty_with_schema(num_rows());
}

std::unique_ptr<Chunk> Chunk::clone_empty_with_schema(size_t size) const {
    Columns columns(_columns.size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        columns[i] = _columns[i]->clone_empty();
        columns[i]->reserve(size);
    }
    return std::make_unique<Chunk>(columns, _schema);
}

std::unique_ptr<Chunk> Chunk::clone_empty_with_tuple() const {
    return clone_empty_with_tuple(num_rows());
}

std::unique_ptr<Chunk> Chunk::clone_empty_with_tuple(size_t size) const {
    Columns columns(_columns.size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        columns[i] = _columns[i]->clone_empty();
        columns[i]->reserve(size);
    }
    return std::make_unique<Chunk>(columns, _slot_id_to_index, _tuple_id_to_index);
}

std::unique_ptr<Chunk> Chunk::clone_unique() const {
    std::unique_ptr<Chunk> chunk = clone_empty_with_tuple(0);
    for (size_t idx = 0; idx < _columns.size(); idx++) {
        ColumnPtr column = _columns[idx]->clone_shared();
        chunk->_columns[idx] = std::move(column);
    }
    chunk->_owner_info = _owner_info;
    chunk->_extra_data = std::move(_extra_data);
    chunk->check_or_die();
    return chunk;
}

void Chunk::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK_EQ(_columns.size(), src.columns().size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->append_selective(*src.columns()[i].get(), indexes, from, size);
    }
}

void Chunk::rolling_append_selective(Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    size_t num_columns = _columns.size();
    DCHECK_EQ(num_columns, src.columns().size());

    for (size_t i = 0; i < num_columns; ++i) {
        _columns[i]->append_selective(*src.columns()[i].get(), indexes, from, size);
        src.columns()[i].reset();
    }
}

size_t Chunk::filter(const Buffer<uint8_t>& selection, bool force) {
    if (!force && SIMD::count_zero(selection) == 0) {
        return num_rows();
    }
    for (auto& column : _columns) {
        column->filter(selection);
    }
    return num_rows();
}

size_t Chunk::filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) {
    for (auto& column : _columns) {
        column->filter_range(selection, from, to);
    }
    return num_rows();
}

DatumTuple Chunk::get(size_t n) const {
    DatumTuple res;
    res.reserve(_columns.size());
    for (const auto& column : _columns) {
        res.append(column->get(n));
    }
    return res;
}

size_t Chunk::memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _columns) {
        memory_usage += column->memory_usage();
    }
    return memory_usage;
}

size_t Chunk::container_memory_usage() const {
    size_t container_memory_usage = 0;
    for (const auto& column : _columns) {
        container_memory_usage += column->container_memory_usage();
    }
    return container_memory_usage;
}

size_t Chunk::reference_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, num_rows()) << "Range error";
    size_t reference_memory_usage = 0;
    for (const auto& column : _columns) {
        reference_memory_usage += column->reference_memory_usage(from, size);
    }
    return reference_memory_usage;
}

size_t Chunk::bytes_usage() const {
    return bytes_usage(0, num_rows());
}

size_t Chunk::bytes_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, num_rows()) << "Range error";
    size_t bytes_usage = 0;
    for (const auto& column : _columns) {
        bytes_usage += column->byte_size(from, size);
    }
    return bytes_usage;
}

#ifndef NDEBUG
void Chunk::check_or_die() {
    if (_columns.empty()) {
        CHECK(_schema == nullptr || _schema->fields().empty());
        CHECK(_cid_to_index.empty());
        CHECK(_slot_id_to_index.empty());
        CHECK(_tuple_id_to_index.empty());
    } else {
        for (const ColumnPtr& c : _columns) {
            CHECK_EQ(num_rows(), c->size());
            c->check_or_die();
        }
    }

    if (_schema != nullptr) {
        for (const auto& kv : _cid_to_index) {
            ColumnId cid = kv.first;
            size_t idx = kv.second;
            CHECK_LT(idx, _columns.size());
            CHECK_LT(idx, _schema->num_fields());
            CHECK_EQ(cid, _schema->field(idx)->id());
        }
    }
}
#endif

std::string Chunk::debug_row(size_t index) const {
    std::stringstream os;
    os << "[";
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->debug_item(index);
        os << ", ";
    }
    os << _columns[_columns.size() - 1]->debug_item(index) << "]";
    return os.str();
}

std::string Chunk::debug_columns() const {
    std::stringstream os;
    os << "nullable[";
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->is_nullable();
        os << ", ";
    }
    os << _columns[_columns.size() - 1]->is_nullable() << "]";
    os << " const[";
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->is_constant();
        os << ", ";
    }
    os << _columns[_columns.size() - 1]->is_constant() << "]";
    return os.str();
}

void Chunk::merge(Chunk&& src) {
    DCHECK_EQ(src.num_rows(), num_rows());
    for (auto& it : src._slot_id_to_index) {
        SlotId slot_id = it.first;
        size_t index = it.second;
        ColumnPtr& c = src._columns[index];
        append_column(c, slot_id);
    }
}

void Chunk::append(const Chunk& src, size_t offset, size_t count) {
    DCHECK_EQ(num_columns(), src.num_columns());
    const size_t n = src.num_columns();
    for (size_t i = 0; i < n; i++) {
        ColumnPtr& c = get_column_by_index(i);
        c->append(*src.get_column_by_index(i), offset, count);
    }
}

void Chunk::append_safe(const Chunk& src, size_t offset, size_t count) {
    DCHECK_EQ(num_columns(), src.num_columns());
    const size_t n = src.num_columns();
    size_t cur_rows = num_rows();

    for (size_t i = 0; i < n; i++) {
        ColumnPtr& c = get_column_by_index(i);
        if (c->size() == cur_rows) {
            c->append(*src.get_column_by_index(i), offset, count);
        }
    }
}

void Chunk::reserve(size_t cap) {
    for (auto& c : _columns) {
        c->reserve(cap);
    }
}

bool Chunk::has_const_column() const {
    for (const auto& c : _columns) {
        if (c->is_constant()) {
            return true;
        }
    }
    return false;
}

} // namespace starrocks
