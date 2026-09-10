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

#include "column/geo_column.h"

#include <stdexcept>

namespace starrocks {
namespace {

[[noreturn]] void unsupported(const char* operation) {
    throw std::runtime_error(std::string("GeoColumn does not support ") + operation);
}

} // namespace

GeoColumn::GeoColumn(GeoColumnDescriptor descriptor, GeoWkbLimits limits)
        : _descriptor(std::move(descriptor)), _limits(limits) {
    if (_descriptor.storage.encoding != GEO_ENCODING_WKB)
        throw std::invalid_argument("GeoColumn requires WKB encoding");
}

const GeoColumn& GeoColumn::_source(const Column& src) const {
    // Column's copy API requires an identical physical type; do not introduce SQL type dispatch here.
    const auto& geo = down_cast<const GeoColumn&>(src);
    if (_descriptor != geo._descriptor) throw std::invalid_argument("GeoColumn descriptor mismatch");
    return geo;
}

void GeoColumn::append_wkb(Slice wkb) {
    clear_wkb_cache();
    // BinaryColumn may reallocate its buffer. Own a copy when appending a slice from ourselves.
    // External ingestion should use batch column copies once a GeoColumn exists.
    const auto begin = reinterpret_cast<uintptr_t>(_data->get_string_begin());
    const auto pointer = reinterpret_cast<uintptr_t>(wkb.data);
    if (wkb.size != 0 && pointer >= begin && pointer - begin < _data->get_immutable_bytes().size()) {
        const std::string copy(wkb.data, wkb.size);
        _data->append(Slice(copy));
    } else {
        _data->append(wkb);
    }
}

StatusOr<GeoWkbInfo> GeoColumn::inspect_wkb(size_t row) {
    if (row >= size()) return Status::InvalidArgument("GeoColumn row out of range");
    if (_cache && _cache->row == row) return _cache->info;
    ASSIGN_OR_RETURN(auto info, inspect_geo_wkb(get_wkb(row), _limits));
    _cache = CachedWkb{row, info};
    return info;
}

void GeoColumn::append_wkb_batch(const Slice* values, size_t count) {
    clear_wkb_cache();
    (void)_data->append_strings(values, count);
}

void GeoColumn::resize(size_t count) {
    clear_wkb_cache();
    _data->resize(count);
}

void GeoColumn::assign(size_t count, size_t row) {
    clear_wkb_cache();
    _data->assign(count, row);
}

void GeoColumn::remove_first_n_values(size_t count) {
    clear_wkb_cache();
    _data->remove_first_n_values(count);
}

void GeoColumn::append(const Column& src, size_t offset, size_t count) {
    if (&src == this) {
        auto copy = clone();
        append(*copy, offset, count);
        return;
    }
    const auto& geo = _source(src);
    clear_wkb_cache();
    _data->append(*geo._data, offset, count);
}

void GeoColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t count) {
    if (&src == this) {
        auto copy = clone();
        append_selective(*copy, indexes, from, count);
        return;
    }
    const auto& geo = _source(src);
    clear_wkb_cache();
    _data->append_selective(*geo._data, indexes, from, count);
}

void GeoColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t count) {
    if (&src == this) {
        auto copy = clone();
        append_value_multiple_times(*copy, index, count);
        return;
    }
    const auto& geo = _source(src);
    clear_wkb_cache();
    _data->append_value_multiple_times(*geo._data, index, count);
}

void GeoColumn::append_value_multiple_times(const void* value, size_t count) {
    // Own the source slice before the first append in case it aliases this column.
    const auto slice = *static_cast<const Slice*>(value);
    const std::string copy(slice.data, slice.size);
    clear_wkb_cache();
    const Slice owned(copy);
    _data->append_value_multiple_times(&owned, count);
}

void GeoColumn::append_default(size_t count) {
    clear_wkb_cache();
    _data->append_default(count);
}

void GeoColumn::fill_default(const Filter& filter) {
    clear_wkb_cache();
    _data->fill_default(filter);
}

void GeoColumn::update_rows(const Column& src, const uint32_t* indexes) {
    if (&src == this) {
        auto copy = clone();
        update_rows(*copy, indexes);
        return;
    }
    const auto& geo = _source(src);
    clear_wkb_cache();
    _data->update_rows(*geo._data, indexes);
}

size_t GeoColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    clear_wkb_cache();
    return _data->filter_range(filter, from, to);
}

MutableColumnPtr GeoColumn::clone_empty() const {
    return create(_descriptor, _limits);
}

MutableColumnPtr GeoColumn::clone() const {
    auto result = create(_descriptor, _limits);
    result->_data->append(*_data, 0, size());
    result->_delete_state = _delete_state;
    return result;
}

void GeoColumn::swap_column(Column& rhs) {
    auto& geo = down_cast<GeoColumn&>(rhs);
    using std::swap;
    swap(_descriptor, geo._descriptor);
    swap(_limits, geo._limits);
    swap(_data, geo._data);
    swap(_delete_state, geo._delete_state);
    clear_wkb_cache();
    geo.clear_wkb_cache();
}

void GeoColumn::reset_column() {
    Column::reset_column();
    clear_wkb_cache();
    _data->reset_column();
}

size_t GeoColumn::container_memory_usage() const {
    return _data->memory_usage() + _descriptor.type.crs.capacity() + 1 + sizeof(_cache);
}

StatusOr<MutableColumnPtr> GeoColumn::upgrade_if_overflow() {
    RETURN_IF_ERROR(capacity_limit_reached());
    return MutableColumnPtr{};
}

std::string GeoColumn::debug_item(size_t row) const {
    return fmt::format("geo[{} bytes]", get_wkb(row).size);
}

int GeoColumn::compare_at(size_t, size_t, const Column&, int) const {
    unsupported("comparison");
}
int64_t GeoColumn::xor_checksum(uint32_t, uint32_t) const {
    unsupported("checksum");
}
void GeoColumn::put_mysql_row_buffer(MysqlRowBuffer*, size_t, bool) const {
    unsupported("public rendering");
}
uint32_t GeoColumn::max_one_element_serialize_size() const {
    unsupported("serialization");
}
uint32_t GeoColumn::serialize(size_t, uint8_t*) const {
    unsupported("serialization");
}
uint32_t GeoColumn::serialize_default(uint8_t*) const {
    unsupported("serialization");
}
uint32_t GeoColumn::serialize_size(size_t) const {
    unsupported("serialization");
}
void GeoColumn::serialize_batch(uint8_t*, Buffer<uint32_t>&, size_t, uint32_t) const {
    unsupported("serialization");
}
const uint8_t* GeoColumn::deserialize_and_append(const uint8_t*) {
    unsupported("deserialization");
}
void GeoColumn::deserialize_and_append_batch(Buffer<Slice>&, size_t) {
    unsupported("deserialization");
}
void GeoColumn::deserialize_and_append_batch_nullable(Buffer<Slice>&, size_t, Buffer<uint8_t>&, bool&) {
    unsupported("deserialization");
}

} // namespace starrocks
