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

#include "column/segmented_chunk.h"

#include <algorithm>
#include <type_traits>
#include <utility>

#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "gutil/strings/fastmem.h"

namespace starrocks {

template <class ColumnT>
inline constexpr bool is_segmented_object_column =
        std::is_same_v<ColumnT, ArrayColumn> || std::is_same_v<ColumnT, StructColumn> ||
        std::is_same_v<ColumnT, MapColumn> || std::is_same_v<ColumnT, JsonColumn> ||
        std::is_same_v<ColumnT, VariantColumn> || std::is_same_v<ObjectColumn<typename ColumnT::ValueType>, ColumnT>;

// Selective-copy data from SegmentedColumn according to provided index.
class SegmentedColumnSelectiveCopy final : public ColumnVisitorAdapter<SegmentedColumnSelectiveCopy> {
public:
    SegmentedColumnSelectiveCopy(SegmentedColumnPtr segment_column, const uint32_t* indexes, uint32_t from,
                                 uint32_t size)
            : ColumnVisitorAdapter(this),
              _segment_column(std::move(segment_column)),
              _indexes(indexes),
              _from(from),
              _size(size) {}

    template <class T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        using ColumnT = FixedLengthColumn<T>;
        using ContainerT = typename ColumnT::Container;
        using ImmContainerT = typename ColumnT::ImmContainer;

        _result = column.clone_empty();
        auto* output = down_cast<ColumnT*>(_result->as_mutable_raw_ptr());
        const size_t segment_size = _segment_column->segment_size();

        std::vector<ImmContainerT> imm_containers;
        std::vector<const T*> buffers;
        auto columns = _segment_column->columns();
        for (auto& seg_column : columns) {
            auto imm_data = ColumnHelper::as_column<ColumnT>(seg_column)->immutable_data();
            imm_containers.push_back(imm_data);
            buffers.push_back(imm_data.data());
        }

        ContainerT& output_items = output->get_data();
        output_items.resize(_size);
        size_t from = _from;
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            DCHECK_LT(segment_id, columns.size());
            DCHECK_LT(segment_offset, columns[segment_id]->size());

            output_items[i] = buffers[segment_id][segment_offset];
        }
        return {};
    }

    // Implementation refers to BinaryColumn::append_selective.
    template <class Offset>
    Status do_visit(const BinaryColumnBase<Offset>& column) {
        using ColumnT = BinaryColumnBase<Offset>;
        using Byte = typename ColumnT::Byte;
        using Offsets = typename ColumnT::Offsets;

        _result = column.clone_empty();
        auto* output = down_cast<ColumnT*>(_result->as_mutable_raw_ptr());
        auto& output_offsets = output->get_offset();
        auto& output_bytes = output->get_bytes();
        const size_t segment_size = _segment_column->segment_size();

        auto columns = _segment_column->columns();
        std::vector<const Byte*> input_bytes;
        std::vector<const Offsets*> input_offsets;
        for (auto& seg_column : columns) {
            auto col_ptr = ColumnHelper::as_column<ColumnT>(seg_column);
            input_bytes.push_back(col_ptr->raw_bytes());
            input_offsets.push_back(&col_ptr->get_offset());
        }

#ifndef NDEBUG
        for (auto& src_col : columns) {
            src_col->check_or_die();
        }
#endif

        output_offsets.resize(_size + 1);
        size_t num_bytes = 0;
        size_t from = _from;
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            DCHECK_LT(segment_id, columns.size());
            DCHECK_LT(segment_offset, columns[segment_id]->size());

            const Offsets& src_offsets = *input_offsets[segment_id];
            Offset str_size = src_offsets[segment_offset + 1] - src_offsets[segment_offset];

            output_offsets[i + 1] = output_offsets[i] + str_size;
            num_bytes += str_size;
        }
        output_bytes.resize(num_bytes);

        Byte* dest_bytes = output_bytes.data();
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            const Byte* src_bytes = input_bytes[segment_id];
            const Offsets& src_offsets = *input_offsets[segment_id];
            Offset str_size = src_offsets[segment_offset + 1] - src_offsets[segment_offset];
            const Byte* str_data = src_bytes + src_offsets[segment_offset];

            strings::memcpy_inlined(dest_bytes + output_offsets[i], str_data, str_size);
        }

#ifndef NDEBUG
        output->check_or_die();
#endif

        return {};
    }

    template <class ColumnT>
    typename std::enable_if_t<is_segmented_object_column<ColumnT>, Status> do_visit(const ColumnT& column) {
        _result = column.clone_empty();
        auto* output = down_cast<ColumnT*>(_result->as_mutable_raw_ptr());
        const size_t segment_size = _segment_column->segment_size();
        output->reserve(_size);

        auto columns = _segment_column->columns();
        size_t from = _from;
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            output->append(*columns[segment_id], segment_offset, 1);
        }
        return {};
    }

    Status do_visit(const NullableColumn& column) {
        Columns data_columns, null_columns;
        for (auto& column : _segment_column->columns()) {
            NullableColumn::Ptr nullable = ColumnHelper::as_column<NullableColumn>(column);
            data_columns.emplace_back(nullable->data_column());
            null_columns.emplace_back(nullable->null_column());
        }

        auto segmented_data_column = std::make_shared<SegmentedColumn>(data_columns, _segment_column->segment_size());
        SegmentedColumnSelectiveCopy copy_data(segmented_data_column, _indexes, _from, _size);
        (void)data_columns[0]->accept(&copy_data);
        auto segmented_null_column = std::make_shared<SegmentedColumn>(null_columns, _segment_column->segment_size());
        SegmentedColumnSelectiveCopy copy_null(segmented_null_column, _indexes, _from, _size);
        (void)null_columns[0]->accept(&copy_null);
        _result = NullableColumn::create(copy_data.result(), ColumnHelper::as_column<NullColumn>(copy_null.result()));

        return {};
    }

    Status do_visit(const AdaptiveNullableColumn& column) {
        return Status::NotSupported("AdaptiveNullableColumn is not supported in SegmentedColumnSelectiveCopy");
    }

    Status do_visit(const ConstColumn& column) { return Status::NotSupported("SegmentedColumnVisitor"); }

    ColumnPtr result() { return _result; }

private:
    __attribute__((always_inline)) std::pair<size_t, size_t> _segment_address(size_t idx, size_t segment_size) {
        size_t segment_id = idx / segment_size;
        size_t segment_offset = idx % segment_size;
        return {segment_id, segment_offset};
    }

    SegmentedColumnPtr _segment_column;
    ColumnPtr _result;
    const uint32_t* _indexes;
    uint32_t _from;
    uint32_t _size;
};

SegmentedColumn::SegmentedColumn(const SegmentedChunkPtr& chunk, size_t column_index)
        : _chunk(chunk), _column_index(column_index), _segment_size(chunk->segment_size()) {}

SegmentedColumn::SegmentedColumn(Columns columns, size_t segment_size)
        : _segment_size(segment_size), _cached_columns(std::move(columns)) {}

ColumnPtr SegmentedColumn::clone_selective(const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (num_segments() == 1) {
        auto first = columns()[0];
        auto result = first->clone_empty();
        result->append_selective(*first, indexes, from, size);
        return result;
    } else {
        SegmentedColumnSelectiveCopy visitor(shared_from_this(), indexes, from, size);
        (void)columns()[0]->accept(&visitor);
        return visitor.result();
    }
}

ColumnPtr SegmentedColumn::materialize() const {
    auto actual_columns = columns();
    if (actual_columns.empty()) {
        return {};
    }
    MutableColumnPtr result = actual_columns[0]->clone_empty();
    for (size_t i = 0; i < actual_columns.size(); i++) {
        result->append(*actual_columns[i]);
    }
    return result;
}

size_t SegmentedColumn::segment_size() const {
    return _segment_size;
}

size_t SegmentedColumn::num_segments() const {
    return _chunk.lock()->num_segments();
}

bool SegmentedColumn::is_nullable() const {
    return columns()[0]->is_nullable();
}

bool SegmentedColumn::has_null() const {
    for (auto& column : columns()) {
        RETURN_IF(column->has_null(), true);
    }
    return false;
}

size_t SegmentedColumn::size() const {
    size_t result = 0;
    for (auto& column : columns()) {
        result += column->size();
    }
    return result;
}

Columns SegmentedColumn::columns() const {
    if (!_cached_columns.empty()) {
        return _cached_columns;
    }
    Columns columns;
    for (auto& segment : _chunk.lock()->segments()) {
        columns.push_back(segment->get_column_by_index(_column_index));
    }
    return columns;
}

void SegmentedColumn::upgrade_to_nullable() {
    for (auto& segment : _chunk.lock()->segments()) {
        auto& column = segment->get_column_by_index(_column_index);
        column = NullableColumn::wrap_if_necessary(std::move(column));
    }
}

SegmentedChunk::SegmentedChunk(size_t segment_size) : _segment_size(segment_size) {
    _segments.resize(1);
    _segments[0] = std::make_shared<Chunk>();
}

SegmentedChunkPtr SegmentedChunk::create(size_t segment_size) {
    return std::make_shared<SegmentedChunk>(segment_size);
}

void SegmentedChunk::append_column(ColumnPtr column, SlotId slot_id) {
    DCHECK_EQ(_segments.size(), 1);
    _segments[0]->append_column(std::move(column), slot_id);
}

void SegmentedChunk::append_chunk(const ChunkPtr& chunk, const std::vector<SlotId>& slots) {
    ChunkPtr open_segment = _segments.back();
    size_t append_rows = chunk->num_rows();
    size_t append_index = 0;
    while (append_rows > 0) {
        size_t open_segment_append_rows = std::min(_segment_size - open_segment->num_rows(), append_rows);
        for (int i = 0; i < slots.size(); i++) {
            SlotId slot = slots[i];
            ColumnPtr column = chunk->get_column_by_slot_id(slot);
            open_segment->get_column_raw_ptr_by_index(i)->append(*column, append_index, open_segment_append_rows);
        }
        append_index += open_segment_append_rows;
        append_rows -= open_segment_append_rows;
        if (open_segment->num_rows() == _segment_size) {
            open_segment->check_or_die();
            open_segment = open_segment->clone_empty();
            _segments.emplace_back(open_segment);
        }
    }
}

void SegmentedChunk::append_chunk(const ChunkPtr& chunk) {
    ChunkPtr open_segment = _segments.back();
    size_t append_rows = chunk->num_rows();
    size_t append_index = 0;
    while (append_rows > 0) {
        size_t open_segment_append_rows = std::min(_segment_size - open_segment->num_rows(), append_rows);
        open_segment->append_safe(*chunk, append_index, open_segment_append_rows);
        append_index += open_segment_append_rows;
        append_rows -= open_segment_append_rows;
        if (open_segment->num_rows() == _segment_size) {
            open_segment->check_or_die();
            open_segment = open_segment->clone_empty();
            _segments.emplace_back(open_segment);
        }
    }
}

void SegmentedChunk::append(const SegmentedChunkPtr& chunk, size_t offset) {
    auto& input_segments = chunk->segments();
    size_t segment_index = offset / chunk->_segment_size;
    size_t segment_offset = offset % chunk->_segment_size;
    for (size_t i = segment_index; i < chunk->num_segments(); i++) {
        if (i == segment_index && segment_offset > 0) {
            auto cutoff = input_segments[i]->clone_empty();
            size_t count = input_segments[i]->num_rows() - segment_offset;
            cutoff->append(*input_segments[i], segment_offset, count);
            append_chunk(std::move(cutoff));
        } else {
            append_chunk(input_segments[i]);
        }
    }
    for (auto& segment : _segments) {
        segment->check_or_die();
    }
}

void SegmentedChunk::build_columns() {
    DCHECK(_segments.size() >= 1);
    size_t num_columns = _segments[0]->num_columns();
    for (int i = 0; i < num_columns; i++) {
        _columns.emplace_back(std::make_shared<SegmentedColumn>(shared_from_this(), i));
    }
}

size_t SegmentedChunk::memory_usage() const {
    size_t result = 0;
    for (auto& chunk : _segments) {
        result += chunk->memory_usage();
    }
    return result;
}

size_t SegmentedChunk::num_rows() const {
    size_t result = 0;
    for (auto& chunk : _segments) {
        result += chunk->num_rows();
    }
    return result;
}

SegmentedColumnPtr SegmentedChunk::get_column_by_slot_id(SlotId slot_id) {
    DCHECK(!!_segments[0]);
    auto& map = _segments[0]->get_slot_id_to_index_map();
    auto iter = map.find(slot_id);
    if (iter == map.end()) {
        return nullptr;
    }
    return _columns[iter->second];
}

const SegmentedColumns& SegmentedChunk::columns() const {
    return _columns;
}

SegmentedColumns& SegmentedChunk::columns() {
    return _columns;
}

Status SegmentedChunk::upgrade_if_overflow() {
    for (auto& chunk : _segments) {
        RETURN_IF_ERROR(chunk->upgrade_if_overflow());
    }
    return {};
}

Status SegmentedChunk::downgrade() {
    for (auto& chunk : _segments) {
        RETURN_IF_ERROR(chunk->downgrade());
    }
    return {};
}

bool SegmentedChunk::has_large_column() const {
    for (auto& chunk : _segments) {
        if (chunk->has_large_column()) {
            return true;
        }
    }
    return false;
}

size_t SegmentedChunk::num_segments() const {
    return _segments.size();
}

const std::vector<ChunkPtr>& SegmentedChunk::segments() const {
    return _segments;
}

std::vector<ChunkPtr>& SegmentedChunk::segments() {
    return _segments;
}

ChunkUniquePtr SegmentedChunk::clone_empty(size_t reserve) {
    return _segments[0]->clone_empty(reserve);
}

void SegmentedChunk::reset() {
    for (auto& chunk : _segments) {
        chunk->reset();
    }
}

void SegmentedChunk::check_or_die() {
    for (auto& chunk : _segments) {
        chunk->check_or_die();
    }
}

size_t SegmentedChunk::segment_size() const {
    return _segment_size;
}

} // namespace starrocks
