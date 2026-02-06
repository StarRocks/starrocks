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

#include "base/simd/simd.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "gen_cpp/data.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "util/coding.h"

namespace starrocks {

Chunk::Chunk() {
    _slot_id_to_index.reserve(4);
}

Status Chunk::upgrade_if_overflow() {
    for (auto& column : _columns) {
        auto mutable_column = column->as_mutable_ptr();
        auto ret = mutable_column->upgrade_if_overflow();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    }
    return Status::OK();
}

Status Chunk::downgrade() {
    for (auto& column : _columns) {
        auto mutable_column = column->as_mutable_ptr();
        auto ret = mutable_column->downgrade();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    }
    return Status::OK();
}

bool Chunk::has_large_column() const {
    for (const auto& column : _columns) {
        if (column != nullptr && column->has_large_column()) {
            return true;
        }
    }
    return false;
}

Chunk::Chunk(Columns&& columns, SchemaPtr schema) : Chunk(std::move(columns), std::move(schema), nullptr) {}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns&& columns, SlotHashMap slot_map) : Chunk(std::move(columns), std::move(slot_map), nullptr) {}

Chunk::Chunk(Columns&& columns, SchemaPtr schema, ChunkExtraDataPtr extra_data)
        : _schema(std::move(schema)), _extra_data(std::move(extra_data)) {
    size_t idx = 0;
    for (auto& column : columns) {
        _append_column_checked(idx++, std::move(column));
    }
    // bucket size cannot be 0.
    _cid_to_index.reserve(std::max<size_t>(1, columns.size() * 2));
    _slot_id_to_index.reserve(std::max<size_t>(1, _columns.size() * 2));
    rebuild_cid_index();
    check_or_die();
}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns&& columns, SlotHashMap slot_map, ChunkExtraDataPtr extra_data)
        : _slot_id_to_index(std::move(slot_map)), _extra_data(std::move(extra_data)) {
    // when use _slot_id_to_index, we don't need to rebuild_cid_index
    size_t idx = 0;
    for (auto& column : columns) {
        _append_column_checked(idx++, std::move(column));
    }
}

Chunk::Chunk(MutableChunk&& other)
        : _columns(ColumnHelper::to_columns(std::move(other._columns))),
          _schema(std::move(other._schema)),
          _extra_data(std::move(other._extra_data)) {
    _slot_id_to_index = std::move(other._slot_id_to_index);
    _cid_to_index = std::move(other._cid_to_index);
    _delete_state = other._delete_state;
    _owner_info = other._owner_info;
    check_or_die();
}

Chunk& Chunk::operator=(MutableChunk&& other) {
    _columns = ColumnHelper::to_columns(std::move(other._columns));
    _schema = std::move(other._schema);
    _extra_data = std::move(other._extra_data);
    _slot_id_to_index = std::move(other._slot_id_to_index);
    _cid_to_index = std::move(other._cid_to_index);
    _delete_state = other._delete_state;
    _owner_info = other._owner_info;
    check_or_die();
    return *this;
}

void Chunk::reset() {
    for (auto& c : _columns) {
        c->as_mutable_raw_ptr()->reset_column();
    }
    _delete_state = DEL_NOT_SATISFIED;
    if (_extra_data != nullptr) {
        _extra_data.reset();
    }
}

void Chunk::swap_chunk(Chunk& other) {
    _columns.swap(other._columns);
    _schema.swap(other._schema);
    _cid_to_index.swap(other._cid_to_index);
    _slot_id_to_index.swap(other._slot_id_to_index);
    std::swap(_delete_state, other._delete_state);
    _extra_data.swap(other._extra_data);
}

void Chunk::set_num_rows(size_t count) {
    for (auto& c : _columns) {
        c->as_mutable_raw_ptr()->resize(count);
    }
}

void Chunk::update_rows(const Chunk& src, const uint32_t* indexes) {
    DCHECK(_columns.size() == src.num_columns());
    for (int i = 0; i < _columns.size(); i++) {
        _columns[i]->as_mutable_raw_ptr()->update_rows(*src.columns()[i], indexes);
    }
}

std::string_view Chunk::get_column_name(size_t idx) const {
    DCHECK_LT(idx, _columns.size());
    return _schema->field(idx)->name();
}

void Chunk::append_column(ColumnPtr&& column, const FieldPtr& field) {
    DCHECK(!_cid_to_index.contains(field->id()));
    _cid_to_index[field->id()] = _columns.size();
    _append_column_checked(_columns.size(), std::move(column));
    _schema->append(field);
    check_or_die();
}

void Chunk::append_column(const ColumnPtr& column, ColumnId column_id, [[maybe_unused]] bool is_column_id) {
    DCHECK(is_column_id);
    DCHECK(!_cid_to_index.contains(column_id));
    _cid_to_index[column_id] = _columns.size();
    _append_column_checked(_columns.size(), column);
    check_or_die();
}

void Chunk::append_column(const ColumnPtr& column, const FieldPtr& field) {
    DCHECK(!_cid_to_index.contains(field->id()));
    _cid_to_index[field->id()] = _columns.size();
    _append_column_checked(_columns.size(), column);
    _schema->append(field);
    check_or_die();
}

void Chunk::append_vector_column(ColumnPtr&& column, const FieldPtr& field, SlotId slot_id) {
    DCHECK(!_cid_to_index.contains(field->id()));
    _cid_to_index[field->id()] = _columns.size();
    _slot_id_to_index[slot_id] = _columns.size();
    _append_column_checked(_columns.size(), std::move(column));
    _schema->append(field);
    check_or_die();
}

void Chunk::append_column(ColumnPtr&& column, SlotId slot_id) {
    DCHECK(!_slot_id_to_index.contains(slot_id)) << "slot_id:" + std::to_string(slot_id) << std::endl;
    if (UNLIKELY(_slot_id_to_index.contains(slot_id))) {
        throw std::runtime_error(fmt::format("slot_id {} already exists", slot_id));
    }
    _slot_id_to_index[slot_id] = _columns.size();
    _append_column_checked(_columns.size(), std::move(column));
    check_or_die();
}

void Chunk::append_column(const ColumnPtr& column, SlotId slot_id) {
    DCHECK(!_slot_id_to_index.contains(slot_id)) << "slot_id:" + std::to_string(slot_id) << std::endl;
    if (UNLIKELY(_slot_id_to_index.contains(slot_id))) {
        throw std::runtime_error(fmt::format("slot_id {} already exists", slot_id));
    }
    _slot_id_to_index[slot_id] = _columns.size();
    _append_column_checked(_columns.size(), column);
    check_or_die();
}

void Chunk::update_column(ColumnPtr&& column, SlotId slot_id) {
    _update_column_checked(_slot_id_to_index[slot_id], std::move(column));
    check_or_die();
}

void Chunk::update_column(ColumnPtr& column, SlotId slot_id) {
    _update_column_checked(_slot_id_to_index[slot_id], column);
    check_or_die();
}

void Chunk::update_column_by_index(ColumnPtr&& column, size_t idx) {
    _update_column_checked(idx, std::move(column));
    check_or_die();
}

void Chunk::append_or_update_column(ColumnPtr&& column, SlotId slot_id) {
    if (_slot_id_to_index.contains(slot_id)) {
        _update_column_checked(_slot_id_to_index[slot_id], std::move(column));
    } else {
        _slot_id_to_index[slot_id] = _columns.size();
        _append_column_checked(_columns.size(), std::move(column));
        // only check it when append a new column
        check_or_die();
    }
}

void Chunk::append_default() {
    for (auto& column : _columns) {
        column->as_mutable_raw_ptr()->append_default();
    }
}

void Chunk::remove_column_by_index(size_t idx) {
    DCHECK_LT(idx, _columns.size());
    auto& column = _columns[idx];
    _column_ptr_set.erase(column.get());
    _columns.erase(_columns.begin() + idx);
    if (_schema != nullptr) {
        _schema->remove(idx);
        rebuild_cid_index();
    }
}

void Chunk::remove_column_by_slot_id(SlotId slot_id) {
    auto iter = _slot_id_to_index.find(slot_id);
    if (iter != _slot_id_to_index.end()) {
        auto idx = iter->second;
        auto& column = _columns[idx];
        _column_ptr_set.erase(column.get());
        _columns.erase(_columns.begin() + idx);
        if (_schema != nullptr) {
            _schema->remove(idx);
            rebuild_cid_index();
        }
        _slot_id_to_index.erase(iter);
        for (auto& tmp_iter : _slot_id_to_index) {
            if (tmp_iter.second > idx) {
                tmp_iter.second--;
            }
        }
    }
}

void Chunk::remove_columns_by_index(const std::vector<size_t>& indexes) {
    DCHECK(std::is_sorted(indexes.begin(), indexes.end()));
    for (size_t i = indexes.size(); i > 0; i--) {
        auto& column = _columns[indexes[i - 1]];
        _column_ptr_set.erase(column.get());
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

ChunkUniquePtr Chunk::clone_empty() const {
    return clone_empty(num_rows());
}

ChunkUniquePtr Chunk::clone_empty(size_t size) const {
    if (_columns.size() == _slot_id_to_index.size()) {
        return clone_empty_with_slot(size);
    } else {
        return clone_empty_with_schema(size);
    }
}

ChunkUniquePtr Chunk::clone_empty_with_slot() const {
    return clone_empty_with_slot(num_rows());
}

ChunkUniquePtr Chunk::clone_empty_with_slot(size_t size) const {
    DCHECK_EQ(_columns.size(), _slot_id_to_index.size());
    Columns columns(_slot_id_to_index.size());
    for (size_t i = 0; i < _slot_id_to_index.size(); i++) {
        auto mutable_col = _columns[i]->clone_empty();
        mutable_col->reserve(size);
        columns[i] = std::move(mutable_col);
    }
    return std::make_unique<Chunk>(std::move(columns), _slot_id_to_index);
}

ChunkUniquePtr Chunk::clone_empty_with_schema() const {
    return clone_empty_with_schema(num_rows());
}

ChunkUniquePtr Chunk::clone_empty_with_schema(size_t size) const {
    Columns columns(_columns.size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        auto mutable_col = _columns[i]->clone_empty();
        mutable_col->reserve(size);
        columns[i] = std::move(mutable_col);
    }
    return std::make_unique<Chunk>(std::move(columns), _schema);
}

ChunkUniquePtr Chunk::clone_unique() const {
    ChunkUniquePtr chunk = clone_empty(0);
    for (size_t idx = 0; idx < _columns.size(); idx++) {
        chunk->_columns[idx] = _columns[idx]->clone();
    }
    chunk->_owner_info = _owner_info;
    if (_extra_data != nullptr) {
        chunk->_extra_data = _extra_data->clone();
    }
    chunk->check_or_die();
    return chunk;
}

void Chunk::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK_EQ(_columns.size(), src.columns().size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->as_mutable_raw_ptr()->append_selective(*src.columns()[i].get(), indexes, from, size);
    }
}

void Chunk::rolling_append_selective(Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    size_t num_columns = _columns.size();
    DCHECK_EQ(num_columns, src.columns().size());
    for (size_t i = 0; i < num_columns; ++i) {
        _columns[i]->as_mutable_raw_ptr()->append_selective(*src.columns()[i].get(), indexes, from, size);
        src.columns()[i].reset();
    }
}

size_t Chunk::filter(const Buffer<uint8_t>& selection, bool force) {
    if (!force && SIMD::count_zero(selection) == 0) {
        return num_rows();
    }
    for (auto& column : _columns) {
        column->as_mutable_raw_ptr()->filter(selection);
    }
    return num_rows();
}

size_t Chunk::filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) {
    for (auto& column : _columns) {
        column->as_mutable_raw_ptr()->filter_range(selection, from, to);
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

VariantTuple Chunk::get(size_t n, const std::vector<uint32_t>& column_indexes) const {
    DCHECK(_schema != nullptr);
    VariantTuple tuple;
    tuple.reserve(column_indexes.size());
    for (uint32_t i : column_indexes) {
        DCHECK_LT(i, _columns.size());
        tuple.emplace(_schema->field(i)->type(), _columns[i]->get(n));
    }
    return tuple;
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
    } else {
        // ensure all columns are not shared with others
        std::unordered_map<const Column*, ColumnPtr> column_map;
        for (const ColumnPtr& c : _columns) {
            if (!c->is_constant()) {
                CHECK_EQ(num_rows(), c->size());
            }
            c->check_or_die();
            CHECK(!column_map.contains(c.get()))
                    << "Column is shared with others, current column: " << c->debug_string()
                    << ", existing columns: " << column_map[c.get()]->debug_string();
            column_map.emplace(c.get(), c);
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

std::string Chunk::rebuild_csv_row(size_t index, const std::string& delimiter) const {
    std::stringstream os;
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->debug_item(index);
        os << delimiter;
    }
    if (_columns.size() > 0) {
        os << _columns[_columns.size() - 1]->debug_item(index);
    }
    return os.str();
}

std::string Chunk::debug_columns() const {
    std::stringstream os;
    if (_columns.empty()) {
        os << "empty columns[]";
    } else {
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
        os << " name[";
        for (size_t col = 0; col < _columns.size() - 1; ++col) {
            os << _columns[col]->get_name();
            os << ", ";
        }
        os << _columns[_columns.size() - 1]->get_name() << "]";
        os << " size[";
        for (size_t col = 0; col < _columns.size() - 1; ++col) {
            os << _columns[col]->size();
            os << ", ";
        }
        os << _columns[_columns.size() - 1]->size() << "]";
        os << " slot_id_to_index[";
        for (const auto& [slot_id, idx] : _slot_id_to_index) {
            os << slot_id << ":" << idx << ", ";
        }
        os << "]";
        os << " column_id_to_index[";
        for (const auto& [column_id, idx] : _cid_to_index) {
            os << column_id << ":" << idx << ", ";
        }
        os << "]";
    }
    return os.str();
}

void Chunk::merge(Chunk&& src) {
    DCHECK_EQ(src.num_rows(), num_rows());
    for (auto& it : src._slot_id_to_index) {
        SlotId slot_id = it.first;
        size_t index = it.second;
        ColumnPtr& c = src._columns[index];
        append_column(std::move(c), slot_id);
    }
}

void Chunk::append(const Chunk& src, size_t offset, size_t count) {
    DCHECK_EQ(num_columns(), src.num_columns());
    const size_t n = src.num_columns();
    for (size_t i = 0; i < n; i++) {
        _columns[i]->as_mutable_raw_ptr()->append(*src.get_column_by_index(i), offset, count);
    }
}

void Chunk::append_safe(const Chunk& src, size_t offset, size_t count) {
    DCHECK_EQ(num_columns(), src.num_columns());
    const size_t n = src.num_columns();
    size_t cur_rows = num_rows();
    for (size_t i = 0; i < n; i++) {
        if (_columns[i]->size() == cur_rows) {
            _columns[i]->as_mutable_raw_ptr()->append(*src.get_column_by_index(i), offset, count);
        }
    }
}

void Chunk::reserve(size_t cap) {
    for (auto& column : _columns) {
        column->as_mutable_raw_ptr()->reserve(cap);
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

void Chunk::unpack_and_duplicate_const_columns() {
    size_t num_rows = this->num_rows();
    for (size_t i = 0; i < _columns.size(); i++) {
        auto column = _columns[i];
        if (column->is_constant()) {
            auto unpack_column = ColumnHelper::unpack_and_duplicate_const_column(num_rows, std::move(column));
            update_column_by_index(std::move(unpack_column), i);
        }
    }
}

void Chunk::_update_column_checked(size_t idx, const ColumnPtr& ptr) {
    if (_is_column_ptr_shared(ptr.get())) {
        // ensure the column is not shared with other columns in the chunk
        DCHECK_EQ(idx, _column_ptr_set.at(ptr.get()))
                << "new idx: " << idx << ", existed idx: " << _column_ptr_set.at(ptr.get())
                << ", new column: " << ptr->get_name();
    }
    _columns[idx] = ptr;
    _column_ptr_set.emplace(ptr.get(), idx);
}

void Chunk::_update_column_checked(size_t idx, ColumnPtr&& ptr) {
    if (_is_column_ptr_shared(ptr.get())) {
        _columns[idx] = Column::mutate(std::move(ptr));
    } else {
        _columns[idx] = std::move(ptr);
    }
    auto& new_column = _columns[idx];
    _column_ptr_set.emplace(new_column.get(), idx);
}

void Chunk::_append_column_checked(size_t idx, const ColumnPtr& ptr) {
    // ensure the column is not shared with other columns in the chunk
    DCHECK(!_is_column_ptr_shared(ptr.get())) << "new idx: " << idx << ", new column: " << ptr->get_name();
    _columns.emplace_back(ptr);
    _column_ptr_set.emplace(ptr.get(), idx);
}

void Chunk::_append_column_checked(size_t idx, ColumnPtr&& ptr) {
    if (_is_column_ptr_shared(ptr.get())) {
        _columns.emplace_back(Column::mutate(std::move(ptr)));
    } else {
        _columns.emplace_back(std::move(ptr));
    }
    auto& new_column = _columns.back();
    _column_ptr_set.emplace(new_column.get(), idx);
}

MutableChunk::MutableChunk() {
    _slot_id_to_index.reserve(4);
}

Status MutableChunk::upgrade_if_overflow() {
    for (auto& column : _columns) {
        auto ret = column->upgrade_if_overflow();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    }
    return Status::OK();
}

Status MutableChunk::downgrade() {
    for (auto& column : _columns) {
        auto ret = column->downgrade();
        if (!ret.ok()) {
            return ret.status();
        } else if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    }
    return Status::OK();
}

bool MutableChunk::has_large_column() const {
    for (const auto& column : _columns) {
        if (column != nullptr && column->has_large_column()) {
            return true;
        }
    }
    return false;
}

MutableChunk::MutableChunk(MutableColumns columns, SchemaPtr schema)
        : MutableChunk(std::move(columns), std::move(schema), nullptr) {}

MutableChunk::MutableChunk(MutableColumns columns, SlotHashMap slot_map)
        : MutableChunk(std::move(columns), std::move(slot_map), nullptr) {}

MutableChunk::MutableChunk(MutableColumns columns, SchemaPtr schema, ChunkExtraDataPtr extra_data)
        : _columns(std::move(columns)), _schema(std::move(schema)), _extra_data(std::move(extra_data)) {
    _cid_to_index.reserve(std::max<size_t>(1, _columns.size() * 2));
    _slot_id_to_index.reserve(std::max<size_t>(1, _columns.size() * 2));
    rebuild_cid_index();
    check_or_die();
}

MutableChunk::MutableChunk(MutableColumns columns, SlotHashMap slot_map, ChunkExtraDataPtr extra_data)
        : _columns(std::move(columns)), _slot_id_to_index(std::move(slot_map)), _extra_data(std::move(extra_data)) {
    // when use _slot_id_to_index, we don't need to rebuild_cid_index
}

void MutableChunk::reset() {
    for (auto& c : _columns) {
        c->reset_column();
    }
    _delete_state = DEL_NOT_SATISFIED;
    _extra_data.reset();
}

void MutableChunk::swap_chunk(MutableChunk& other) {
    _columns.swap(other._columns);
    _schema.swap(other._schema);
    _cid_to_index.swap(other._cid_to_index);
    _slot_id_to_index.swap(other._slot_id_to_index);
    std::swap(_delete_state, other._delete_state);
    _extra_data.swap(other._extra_data);
}

void MutableChunk::set_num_rows(size_t count) {
    for (auto& c : _columns) {
        c->resize(count);
    }
}

void MutableChunk::update_rows(const Chunk& src, const uint32_t* indexes) {
    DCHECK(_columns.size() == src.num_columns());
    for (int i = 0; i < _columns.size(); i++) {
        _columns[i]->update_rows(*src.columns()[i], indexes);
    }
}

std::string_view MutableChunk::get_column_name(size_t idx) const {
    DCHECK_LT(idx, _columns.size());
    return _schema->field(idx)->name();
}

void MutableChunk::append_column(MutableColumnPtr&& column, const FieldPtr& field) {
    DCHECK(!_cid_to_index.contains(field->id()));
    _cid_to_index[field->id()] = _columns.size();
    _columns.emplace_back(std::move(column));
    _schema->append(field);
    check_or_die();
}

void MutableChunk::append_vector_column(MutableColumnPtr&& column, const FieldPtr& field, SlotId slot_id) {
    DCHECK(!_cid_to_index.contains(field->id()));
    _cid_to_index[field->id()] = _columns.size();
    _slot_id_to_index[slot_id] = _columns.size();
    _columns.emplace_back(std::move(column));
    _schema->append(field);
    check_or_die();
}

void MutableChunk::append_column(MutableColumnPtr&& column, SlotId slot_id) {
    DCHECK(!_slot_id_to_index.contains(slot_id)) << "slot_id:" + std::to_string(slot_id) << std::endl;
    if (UNLIKELY(_slot_id_to_index.contains(slot_id))) {
        throw std::runtime_error(fmt::format("slot_id {} already exists", slot_id));
    }
    _slot_id_to_index[slot_id] = _columns.size();
    _columns.emplace_back(std::move(column));
    check_or_die();
}

void MutableChunk::update_column(MutableColumnPtr&& column, SlotId slot_id) {
    _columns[_slot_id_to_index[slot_id]] = std::move(column);
    check_or_die();
}

void MutableChunk::update_column_by_index(MutableColumnPtr&& column, size_t idx) {
    _columns[idx] = std::move(column);
    check_or_die();
}

void MutableChunk::append_or_update_column(MutableColumnPtr&& column, SlotId slot_id) {
    if (_slot_id_to_index.contains(slot_id)) {
        _columns[_slot_id_to_index[slot_id]] = std::move(column);
    } else {
        _slot_id_to_index[slot_id] = _columns.size();
        _columns.emplace_back(std::move(column));
        // only check it when append a new column
        check_or_die();
    }
}

void MutableChunk::insert_column(size_t idx, MutableColumnPtr&& column, const FieldPtr& field) {
    DCHECK_LT(idx, _columns.size());
    _columns.emplace(_columns.begin() + idx, std::move(column));
    _schema->insert(idx, field);
    rebuild_cid_index();
    check_or_die();
}

void MutableChunk::append_default() {
    for (auto& column : _columns) {
        column->append_default();
    }
}

void MutableChunk::remove_column_by_index(size_t idx) {
    DCHECK_LT(idx, _columns.size());
    _columns.erase(_columns.begin() + idx);
    if (_schema != nullptr) {
        _schema->remove(idx);
        rebuild_cid_index();
    }
}

void MutableChunk::remove_column_by_slot_id(SlotId slot_id) {
    auto iter = _slot_id_to_index.find(slot_id);
    if (iter != _slot_id_to_index.end()) {
        auto idx = iter->second;
        _columns.erase(_columns.begin() + idx);
        if (_schema != nullptr) {
            _schema->remove(idx);
            rebuild_cid_index();
        }
        _slot_id_to_index.erase(iter);
        for (auto& tmp_iter : _slot_id_to_index) {
            if (tmp_iter.second > idx) {
                tmp_iter.second--;
            }
        }
    }
}

void MutableChunk::remove_columns_by_index(const std::vector<size_t>& indexes) {
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

void MutableChunk::rebuild_cid_index() {
    _cid_to_index.clear();
    for (size_t i = 0; i < _schema->num_fields(); i++) {
        _cid_to_index[_schema->field(i)->id()] = i;
    }
}

MutableChunkPtr MutableChunk::clone_empty() const {
    return clone_empty(num_rows());
}

MutableChunkPtr MutableChunk::clone_empty(size_t size) const {
    if (_columns.size() == _slot_id_to_index.size()) {
        return clone_empty_with_slot(size);
    } else {
        return clone_empty_with_schema(size);
    }
}

MutableChunkPtr MutableChunk::clone_empty_with_slot() const {
    return clone_empty_with_slot(num_rows());
}

MutableChunkPtr MutableChunk::clone_empty_with_slot(size_t size) const {
    DCHECK_EQ(_columns.size(), _slot_id_to_index.size());
    MutableColumns columns(_slot_id_to_index.size());
    for (size_t i = 0; i < _slot_id_to_index.size(); i++) {
        auto mutable_col = _columns[i]->clone_empty();
        mutable_col->reserve(size);
        columns[i] = std::move(mutable_col);
    }
    return std::make_shared<MutableChunk>(std::move(columns), _slot_id_to_index);
}

MutableChunkPtr MutableChunk::clone_empty_with_schema() const {
    return clone_empty_with_schema(num_rows());
}

MutableChunkPtr MutableChunk::clone_empty_with_schema(size_t size) const {
    MutableColumns columns(_columns.size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        auto mutable_col = _columns[i]->clone_empty();
        mutable_col->reserve(size);
        columns[i] = std::move(mutable_col);
    }
    return std::make_shared<MutableChunk>(std::move(columns), _schema);
}

MutableChunkPtr MutableChunk::clone_unique() const {
    MutableChunkPtr chunk = clone_empty(0);
    for (size_t idx = 0; idx < _columns.size(); idx++) {
        chunk->_columns[idx] = _columns[idx]->clone();
    }
    chunk->_delete_state = _delete_state;
    chunk->_owner_info = _owner_info;
    if (_extra_data != nullptr) {
        chunk->_extra_data = _extra_data->clone();
    }
    chunk->check_or_die();
    return chunk;
}

void MutableChunk::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK_EQ(_columns.size(), src.columns().size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->append_selective(*src.columns()[i].get(), indexes, from, size);
    }
}

void MutableChunk::rolling_append_selective(Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    size_t num_columns = _columns.size();
    DCHECK_EQ(num_columns, src.columns().size());
    for (size_t i = 0; i < num_columns; ++i) {
        _columns[i]->append_selective(*src.columns()[i].get(), indexes, from, size);
        src.columns()[i].reset();
    }
}

size_t MutableChunk::filter(const Buffer<uint8_t>& selection, bool force) {
    if (!force && SIMD::count_zero(selection) == 0) {
        return num_rows();
    }
    for (auto& column : _columns) {
        column->filter(selection);
    }
    return num_rows();
}

size_t MutableChunk::filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) {
    for (auto& column : _columns) {
        column->filter_range(selection, from, to);
    }
    return num_rows();
}

DatumTuple MutableChunk::get(size_t n) const {
    DatumTuple res;
    res.reserve(_columns.size());
    for (const auto& column : _columns) {
        res.append(column->get(n));
    }
    return res;
}

size_t MutableChunk::memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _columns) {
        memory_usage += column->memory_usage();
    }
    return memory_usage;
}

size_t MutableChunk::container_memory_usage() const {
    size_t container_memory_usage = 0;
    for (const auto& column : _columns) {
        container_memory_usage += column->container_memory_usage();
    }
    return container_memory_usage;
}

size_t MutableChunk::reference_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, num_rows()) << "Range error";
    size_t reference_memory_usage = 0;
    for (const auto& column : _columns) {
        reference_memory_usage += column->reference_memory_usage(from, size);
    }
    return reference_memory_usage;
}

size_t MutableChunk::bytes_usage() const {
    return bytes_usage(0, num_rows());
}

size_t MutableChunk::bytes_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, num_rows()) << "Range error";
    size_t bytes_usage = 0;
    for (const auto& column : _columns) {
        bytes_usage += column->byte_size(from, size);
    }
    return bytes_usage;
}

#ifndef NDEBUG
void MutableChunk::check_or_die() {
    if (_columns.empty()) {
        CHECK(_schema == nullptr || _schema->fields().empty());
        CHECK(_cid_to_index.empty());
        CHECK(_slot_id_to_index.empty());
    } else {
        for (const MutableColumnPtr& c : _columns) {
            if (!c->is_constant()) {
                CHECK_EQ(num_rows(), c->size());
            }
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

std::string MutableChunk::debug_row(size_t index) const {
    std::stringstream os;
    os << "[";
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->debug_item(index);
        os << ", ";
    }
    os << _columns[_columns.size() - 1]->debug_item(index) << "]";
    return os.str();
}

std::string MutableChunk::rebuild_csv_row(size_t index, const std::string& delimiter) const {
    std::stringstream os;
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->debug_item(index);
        os << delimiter;
    }
    if (_columns.size() > 0) {
        os << _columns[_columns.size() - 1]->debug_item(index);
    }
    return os.str();
}

std::string MutableChunk::debug_columns() const {
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

void MutableChunk::merge(MutableChunk&& src) {
    DCHECK_EQ(src.num_rows(), num_rows());
    for (auto& it : src._slot_id_to_index) {
        SlotId slot_id = it.first;
        size_t index = it.second;
        MutableColumnPtr& c = src._columns[index];
        append_column(std::move(c), slot_id);
    }
}

void MutableChunk::append(const Chunk& src, size_t offset, size_t count) {
    DCHECK_EQ(num_columns(), src.num_columns());
    const size_t n = src.num_columns();
    for (size_t i = 0; i < n; i++) {
        _columns[i]->append(*src.get_column_by_index(i), offset, count);
    }
}

void MutableChunk::append_safe(const Chunk& src, size_t offset, size_t count) {
    DCHECK_EQ(num_columns(), src.num_columns());
    const size_t n = src.num_columns();
    size_t cur_rows = num_rows();
    for (size_t i = 0; i < n; i++) {
        auto& column = _columns[i];
        if (column->size() == cur_rows) {
            column->append(*src.get_column_by_index(i), offset, count);
        }
    }
}

void MutableChunk::reserve(size_t cap) {
    for (auto& c : _columns) {
        c->reserve(cap);
    }
}

bool MutableChunk::has_const_column() const {
    for (const auto& c : _columns) {
        if (c->is_constant()) {
            return true;
        }
    }
    return false;
}

void MutableChunk::unpack_and_duplicate_const_columns() {
    size_t num_rows = this->num_rows();
    for (size_t i = 0; i < _columns.size(); i++) {
        auto& column = _columns[i];
        if (column->is_constant()) {
            auto unpack_column = ColumnHelper::unpack_and_duplicate_const_column(num_rows, std::move(column));
            _columns[i] = std::move(unpack_column);
        }
    }
}

Chunk MutableChunk::to_chunk() {
    return Chunk(std::move(*this));
}

MutableChunk::MutableChunk(Chunk&& other)
        : _columns(ColumnHelper::to_mutable_columns(std::move((other._columns)))),
          _schema(std::move(other._schema)),
          _extra_data(std::move(other._extra_data)) {
    _slot_id_to_index = std::move(other._slot_id_to_index);
    _cid_to_index = std::move(other._cid_to_index);
    _delete_state = other._delete_state;
    _owner_info = other._owner_info;
    check_or_die();
}

MutableChunk& MutableChunk::operator=(Chunk&& other) {
    _columns = ColumnHelper::to_mutable_columns(std::move((other._columns)));
    _schema = std::move(other._schema);
    _extra_data = std::move(other._extra_data);
    _slot_id_to_index = std::move(other._slot_id_to_index);
    _cid_to_index = std::move(other._cid_to_index);
    _delete_state = other._delete_state;
    _owner_info = other._owner_info;
    check_or_die();
    return *this;
}

} // namespace starrocks
