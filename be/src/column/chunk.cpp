// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/chunk.h"

#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "gen_cpp/data.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "util/coding.h"

namespace starrocks::vectorized {

Chunk::Chunk() {
    _slot_id_to_index.init(4);
    _tuple_id_to_index.init(1);
}

Chunk::Chunk(Columns columns, SchemaPtr schema) : _columns(std::move(columns)), _schema(std::move(schema)) {
    // bucket size cannot be 0.
    _cid_to_index.init(std::max<size_t>(1, columns.size() * 2));
    _slot_id_to_index.init(std::max<size_t>(1, _columns.size() * 2));
    _tuple_id_to_index.init(1);
    rebuild_cid_index();
    check_or_die();
}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns columns, const butil::FlatMap<SlotId, size_t>& slot_map)
        : _columns(std::move(columns)), _slot_id_to_index(slot_map) {
    // when use _slot_id_to_index, we don't need to rebuild_cid_index
    _tuple_id_to_index.init(1);
}

// TODO: FlatMap don't support std::move
Chunk::Chunk(Columns columns, const butil::FlatMap<SlotId, size_t>& slot_map,
             const butil::FlatMap<SlotId, size_t>& tuple_map)
        : _columns(std::move(columns)), _slot_id_to_index(slot_map), _tuple_id_to_index(tuple_map) {
    // when use _slot_id_to_index, we don't need to rebuild_cid_index
}

void Chunk::reset() {
    for (ColumnPtr& c : _columns) {
        c->reset_column();
    }
    _delete_state = DEL_NOT_SATISFIED;
}

void Chunk::swap_chunk(Chunk& other) {
    _columns.swap(other._columns);
    _schema.swap(other._schema);
    _cid_to_index.swap(other._cid_to_index);
    _slot_id_to_index.swap(other._slot_id_to_index);
    _tuple_id_to_index.swap(other._tuple_id_to_index);
    std::swap(_delete_state, other._delete_state);
}

void Chunk::set_num_rows(size_t count) {
    for (ColumnPtr& c : _columns) {
        c->resize(count);
    }
}

std::string Chunk::get_column_name(size_t idx) const {
    DCHECK_LT(idx, _columns.size());
    return _schema->field(idx)->name();
}

void Chunk::append_column(ColumnPtr column, const FieldPtr& field) {
    DCHECK(_cid_to_index.seek(field->id()) == nullptr);
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

void Chunk::remove_column_by_index(size_t idx) {
    DCHECK_LT(idx, _columns.size());
    _columns.erase(_columns.begin() + idx);
    if (_schema != nullptr) {
        _schema->remove(idx);
        rebuild_cid_index();
    }
}

void Chunk::remove_columns_by_index(const std::vector<size_t>& indexes) {
    DCHECK(std::is_sorted(indexes.begin(), indexes.end()));
    for (int i = indexes.size(); i > 0; i--) {
        _columns.erase(_columns.begin() + indexes[i - 1]);
    }
    if (_schema != nullptr && !indexes.empty()) {
        for (int i = indexes.size(); i > 0; i--) {
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

size_t Chunk::serialize_size() const {
    size_t size = 0;
    for (const auto& column : _columns) {
        size += column->serialize_size();
    }
    size += sizeof(uint32_t) + sizeof(uint32_t); // version + num rows
    return size;
}

void Chunk::serialize(uint8_t* dst) const {
    uint32_t version = 1;
    encode_fixed32_le(dst, version);
    dst += sizeof(uint32_t);

    encode_fixed32_le(dst, num_rows());
    dst += sizeof(uint32_t);

    for (const auto& column : _columns) {
        dst = column->serialize_column(dst);
    }
}

size_t Chunk::serialize_with_meta(starrocks::ChunkPB* chunk) const {
    chunk->clear_slot_id_map();
    chunk->mutable_slot_id_map()->Reserve(static_cast<int>(_slot_id_to_index.size()) * 2);
    for (const auto& kv : _slot_id_to_index) {
        chunk->mutable_slot_id_map()->Add(kv.first);
        chunk->mutable_slot_id_map()->Add(kv.second);
    }

    chunk->clear_tuple_id_map();
    chunk->mutable_tuple_id_map()->Reserve(static_cast<int>(_tuple_id_to_index.size()) * 2);
    for (const auto& kv : _tuple_id_to_index) {
        chunk->mutable_tuple_id_map()->Add(kv.first);
        chunk->mutable_tuple_id_map()->Add(kv.second);
    }

    chunk->clear_is_nulls();
    chunk->mutable_is_nulls()->Reserve(_columns.size());
    for (const auto& column : _columns) {
        chunk->mutable_is_nulls()->Add(column->is_nullable());
    }

    chunk->clear_is_consts();
    chunk->mutable_is_consts()->Reserve(_columns.size());
    for (const auto& column : _columns) {
        chunk->mutable_is_consts()->Add(column->is_constant());
    }

    DCHECK_EQ(_columns.size(), _tuple_id_to_index.size() + _slot_id_to_index.size());

    size_t size = serialize_size();
    chunk->mutable_data()->resize(size);
    serialize((uint8_t*)chunk->mutable_data()->data());
    return size;
}

Status Chunk::deserialize(const uint8_t* src, size_t len, const RuntimeChunkMeta& meta) {
    _slot_id_to_index = meta.slot_id_to_index;
    _tuple_id_to_index = meta.tuple_id_to_index;
    _columns.resize(_slot_id_to_index.size() + _tuple_id_to_index.size());

    const uint8_t* data_start = src;
    uint32_t version = decode_fixed32_le(src);
    DCHECK_EQ(version, 1);
    src += sizeof(uint32_t);

    size_t rows = decode_fixed32_le(src);
    src += sizeof(uint32_t);

    for (size_t i = 0; i < meta.is_nulls.size(); ++i) {
        _columns[i] = ColumnHelper::create_column(meta.types[i], meta.is_nulls[i], meta.is_consts[i], rows);
    }

    for (const auto& column : _columns) {
        src = column->deserialize_column(src);
    }
    const uint8_t* data_end = src;
    size_t read_size = (data_end - data_start);

    if (UNLIKELY(len != read_size)) {
        return Status::InternalError(
                strings::Substitute("deserialize chunk data failed. len: $0, read: $1", len, read_size));
    }
    DCHECK_EQ(rows, num_rows());
    return Status::OK();
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
    int size = num_rows();
    return clone_empty_with_schema(size);
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

void Chunk::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK_EQ(_columns.size(), src.columns().size());
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->append_selective(*src.columns()[i].get(), indexes, from, size);
    }
}

size_t Chunk::filter(const Buffer<uint8_t>& selection) {
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

size_t Chunk::shrink_memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _columns) {
        memory_usage += column->shrink_memory_usage();
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

size_t Chunk::element_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, num_rows()) << "Range error";
    size_t element_memory_usage = 0;
    for (const auto& column : _columns) {
        element_memory_usage += column->element_memory_usage(from, size);
    }
    return element_memory_usage;
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

std::string Chunk::debug_row(uint32_t index) const {
    std::stringstream os;
    os << "[";
    for (size_t col = 0; col < _columns.size() - 1; ++col) {
        os << _columns[col]->debug_item(index);
        os << ", ";
    }
    os << _columns[_columns.size() - 1]->debug_item(index) << "]";
    return os.str();
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

} // namespace starrocks::vectorized
