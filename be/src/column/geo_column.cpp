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

#include <bit>
#include <cstring>
#include <stdexcept>

#include "base/coding.h"
#include "column/column_visitor.h"
#include "column/column_visitor_mutable.h"
#include "column/mysql_row_buffer.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

[[noreturn]] void unsupported(const char* operation) {
    throw std::runtime_error(std::string("GeoColumn does not support ") + operation);
}

Status check_transport_descriptor(const GeoColumnDescriptor& descriptor) {
    const auto& type = descriptor.type;
    const auto& storage = descriptor.storage;
    if (type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY || type.coordinate_system != GEO_COORDINATE_SYSTEM_SPHERICAL ||
        !GeoEdgeAlgorithmPB_IsValid(type.edge_algorithm) || type.edge_algorithm == GEO_EDGE_ALGORITHM_UNKNOWN ||
        type.edge_algorithm == GEO_EDGE_ALGORITHM_PLANAR || type.crs.empty() || type.crs.size() > 65536 ||
        storage.encoding != GEO_ENCODING_WKB || !GeoDimensionPB_IsValid(storage.dimension) ||
        !GeoValidationStatePB_IsValid(storage.validation_state)) {
        return Status::NotSupported("Unsupported GEOGRAPHY transport descriptor");
    }
    return Status::OK();
}

} // namespace

GeoColumn::GeoColumn(size_t size)
        : GeoColumn(GeoColumnDescriptor{{},
                                        {GEO_ENCODING_WKB, GEO_DIMENSION_UNKNOWN, GEO_VALIDATION_STATE_UNVALIDATED}}) {
    resize(size);
}

GeoColumn::GeoColumn(GeoColumnDescriptor descriptor, GeoWkbLimits limits)
        : _descriptor(std::move(descriptor)), _limits(limits) {
    if (_descriptor.storage.encoding != GEO_ENCODING_WKB)
        throw std::invalid_argument("GeoColumn requires WKB encoding");
}

GeoColumn::GeoColumn(const TypeDescriptor& type, size_t size)
        : GeoColumn(GeoColumnDescriptor{type.geo_type.value_or(GeoTypeDescriptor{}),
                                        {GEO_ENCODING_WKB, GEO_DIMENSION_UNKNOWN, GEO_VALIDATION_STATE_UNVALIDATED}}) {
    if (type.geo_type && type.geo_type->logical_type != GEO_LOGICAL_TYPE_UNKNOWN &&
        ((type.type == TYPE_GEOGRAPHY) != (type.geo_type->logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY))) {
        throw std::invalid_argument("GeoColumn primitive/descriptor mismatch");
    }
    resize(size);
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

GeoColumn::ImmContainer GeoColumn::immutable_data() const {
    unsupported("generic scalar access");
}

GeoColumn::ValueType GeoColumn::min_value() {
    unsupported("range bounds");
}

GeoColumn::ValueType GeoColumn::max_value() {
    unsupported("range bounds");
}

std::string GeoColumn::debug_item(size_t) const {
    // Generic fingerprinting uses this as a value representation. A byte count
    // is not a GEO value and must not silently become its hash input.
    unsupported("generic scalar rendering");
}

Status GeoColumn::accept(ColumnVisitor* visitor) const {
    return visitor->visit(*this);
}

Status GeoColumn::accept_mutable(ColumnVisitorMutable* visitor) {
    return visitor->visit(this);
}

int GeoColumn::compare_at(size_t, size_t, const Column&, int) const {
    unsupported("comparison");
}
int64_t GeoColumn::xor_checksum(uint32_t, uint32_t) const {
    unsupported("checksum");
}
void GeoColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t row, bool) const {
    if (_descriptor.type.logical_type != GEO_LOGICAL_TYPE_GEOGRAPHY ||
        _descriptor.storage.encoding != GEO_ENCODING_WKB) {
        unsupported("MySQL output for non-GEOGRAPHY or non-WKB columns");
    }
    const auto wkb = get_wkb(row);
    buf->push_binary(wkb.data, wkb.size);
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

int64_t GeoColumn::serialized_column_size() const {
    return 16 + _descriptor.to_protobuf().ByteSizeLong() + _data->get_immutable_bytes().size() +
           sizeof(uint32_t) * _data->get_offset().size();
}

StatusOr<uint8_t*> GeoColumn::serialize_column(uint8_t* dst) const {
    DCHECK(check_transport_descriptor(_descriptor).ok());
    RETURN_IF_ERROR(_data->is_payload_size_representable());
    const auto descriptor = _descriptor.to_protobuf();
    const auto descriptor_size = descriptor.ByteSizeLong();
    const auto bytes = _data->get_immutable_bytes();
    const auto& offsets = _data->get_offset();
    if (offsets.size() > UINT32_MAX || descriptor_size > 65536) {
        return Status::NotSupported("GEOGRAPHY transport column exceeds format limits");
    }
    // Version, descriptor length, WKB bytes length, offset count; all little endian.
    encode_fixed32_le(dst, 1);
    encode_fixed32_le(dst + 4, descriptor_size);
    encode_fixed32_le(dst + 8, bytes.size());
    encode_fixed32_le(dst + 12, offsets.size());
    dst = descriptor.SerializeWithCachedSizesToArray(dst + 16);
    if (!bytes.empty()) memcpy(dst, bytes.data(), bytes.size());
    dst += bytes.size();
    if constexpr (std::endian::native == std::endian::little) {
        if (!offsets.is_large()) {
            memcpy(dst, offsets.small_storage().data(), offsets.size() * sizeof(uint32_t));
            return dst + offsets.size() * sizeof(uint32_t);
        }
    }
    for (size_t i = 0; i < offsets.size(); ++i, dst += sizeof(uint32_t)) {
        encode_fixed32_le(dst, offsets[i]);
    }
    return dst;
}

StatusOr<const uint8_t*> GeoColumn::deserialize_column(const uint8_t* src, const uint8_t* end) {
    if (end - src < 16) return Status::Corruption("Truncated GEOGRAPHY transport header");
    if (decode_fixed32_le(src) != 1) return Status::NotSupported("Unsupported GEOGRAPHY transport version");
    const uint32_t descriptor_size = decode_fixed32_le(src + 4);
    const uint32_t bytes_size = decode_fixed32_le(src + 8);
    const uint32_t offset_count = decode_fixed32_le(src + 12);
    src += 16;
    const uint64_t total_size = uint64_t(descriptor_size) + bytes_size + uint64_t(offset_count) * sizeof(uint32_t);
    if (descriptor_size > 65536 || offset_count == 0 || total_size > static_cast<uint64_t>(end - src)) {
        return Status::Corruption("Invalid GEOGRAPHY transport lengths");
    }
    GeoColumnDescPB pb;
    if (!pb.ParseFromArray(src, descriptor_size) || !pb.has_type() || !pb.has_storage() ||
        !pb.unknown_fields().empty() || !pb.type().unknown_fields().empty() || !pb.storage().unknown_fields().empty()) {
        return Status::Corruption("Invalid or unsupported GEOGRAPHY transport metadata");
    }
    auto descriptor = GeoColumnDescriptor::from_protobuf(pb);
    RETURN_IF_ERROR(check_transport_descriptor(descriptor));
    // The receiving plan owns semantic identity; only storage metadata comes from the wire.
    if (_descriptor.type != descriptor.type) {
        return Status::Corruption("GEOGRAPHY transport descriptor does not match the receiving type");
    }
    const auto* bytes = src + descriptor_size;
    const auto* offsets = bytes + bytes_size;
    // Allocation is bounded by the already validated input span, not by an unchecked row count.
    Buffer<uint32_t> decoded_offsets;
    decoded_offsets.resize(offset_count);
    if constexpr (std::endian::native == std::endian::little) {
        memcpy(decoded_offsets.data(), offsets, uint64_t(offset_count) * sizeof(uint32_t));
    } else {
        for (uint32_t i = 0; i < offset_count; ++i) {
            decoded_offsets[i] = decode_fixed32_le(offsets + uint64_t(i) * sizeof(uint32_t));
        }
    }
    uint32_t previous = 0;
    for (uint32_t i = 0; i < offset_count; ++i) {
        const auto offset = decoded_offsets[i];
        if ((i == 0 && offset != 0) || offset < previous || offset > bytes_size) {
            return Status::Corruption("Invalid GEOGRAPHY transport offsets");
        }
        previous = offset;
    }
    if (previous != bytes_size) return Status::Corruption("GEOGRAPHY transport offsets do not cover payload");

    // No payload allocation or destination mutation before all metadata/offset checks succeed.
    auto& data = _data->get_bytes();
    data.resize(bytes_size);
    if (bytes_size != 0) memcpy(data.data(), bytes, bytes_size);
    _data->get_offset().set_small_buffer(std::move(decoded_offsets));
    _descriptor = std::move(descriptor);
    clear_wkb_cache();
    return src + total_size;
}

} // namespace starrocks
