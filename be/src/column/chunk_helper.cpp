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

#include "chunk_helper.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/schema.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "simd/simd.h"
#include "storage/olap_type_infra.h"
#include "storage/tablet_schema.h"
#include "storage/type_traits.h"
#include "storage/type_utils.h"
#include "storage/types.h"
#include "util/metrics.h"
#include "util/percentile_value.h"

namespace starrocks {

ChunkAccumulator::ChunkAccumulator(size_t desired_size) : _desired_size(desired_size) {}

void ChunkAccumulator::set_desired_size(size_t desired_size) {
    _desired_size = desired_size;
}

void ChunkAccumulator::reset() {
    _output.clear();
    _tmp_chunk.reset();
    _accumulate_count = 0;
}

Status ChunkAccumulator::push(ChunkPtr&& chunk) {
    size_t input_rows = chunk->num_rows();
    // TODO: optimize for zero-copy scenario
    // Cut the input chunk into pieces if larger than desired
    for (size_t start = 0; start < input_rows;) {
        size_t remain_rows = input_rows - start;
        size_t need_rows = 0;
        if (_tmp_chunk) {
            need_rows = std::min(_desired_size - _tmp_chunk->num_rows(), remain_rows);
            TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
        } else {
            need_rows = std::min(_desired_size, remain_rows);
            _tmp_chunk = chunk->clone_empty(_desired_size);
            TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
        }

        if (_tmp_chunk->num_rows() >= _desired_size) {
            _output.emplace_back(std::move(_tmp_chunk));
        }
        start += need_rows;
    }
    _accumulate_count++;
    return Status::OK();
}

bool ChunkAccumulator::empty() const {
    return _output.empty();
}

bool ChunkAccumulator::reach_limit() const {
    return _accumulate_count >= kAccumulateLimit;
}

ChunkPtr ChunkAccumulator::pull() {
    if (!_output.empty()) {
        auto res = std::move(_output.front());
        _output.pop_front();
        _accumulate_count = 0;
        return res;
    }
    return nullptr;
}

void ChunkAccumulator::finalize() {
    if (_tmp_chunk) {
        _output.emplace_back(std::move(_tmp_chunk));
    }
    _accumulate_count = 0;
}

bool ChunkPipelineAccumulator::_check_json_schema_equallity(const Chunk* one, const Chunk* two) {
    if (one->num_columns() != two->num_columns()) {
        return false;
    }

    for (size_t i = 0; i < one->num_columns(); i++) {
        auto& c1 = one->get_column_by_index(i);
        auto& c2 = two->get_column_by_index(i);
        const auto* a1 = ColumnHelper::get_data_column(c1.get());
        const auto* a2 = ColumnHelper::get_data_column(c2.get());

        if (a1->is_json() && a2->is_json()) {
            auto json1 = down_cast<const JsonColumn*>(a1);
            if (!json1->is_equallity_schema(a2)) {
                return false;
            }
        } else if (a1->is_json() || a2->is_json()) {
            // never hit
            DCHECK_EQ(a1->is_json(), a2->is_json());
            return false;
        }
    }

    return true;
}

void ChunkPipelineAccumulator::push(const ChunkPtr& chunk) {
    chunk->check_or_die();
    DCHECK(_out_chunk == nullptr);
    if (_in_chunk == nullptr) {
        _in_chunk = chunk;
        _mem_usage = chunk->bytes_usage();
    } else if (_in_chunk->num_rows() + chunk->num_rows() > _max_size ||
               _in_chunk->owner_info() != chunk->owner_info() || _in_chunk->owner_info().is_last_chunk() ||
               !_check_json_schema_equallity(chunk.get(), _in_chunk.get())) {
        _out_chunk = std::move(_in_chunk);
        _in_chunk = chunk;
        _mem_usage = chunk->bytes_usage();
    } else {
        _in_chunk->append(*chunk);
        _mem_usage += chunk->bytes_usage();
    }

    if (_out_chunk == nullptr && (_in_chunk->num_rows() >= _max_size * LOW_WATERMARK_ROWS_RATE ||
                                  _mem_usage >= LOW_WATERMARK_BYTES || _in_chunk->owner_info().is_last_chunk())) {
        _out_chunk = std::move(_in_chunk);
        _mem_usage = 0;
    }
}

void ChunkPipelineAccumulator::reset() {
    _in_chunk.reset();
    _out_chunk.reset();
    _mem_usage = 0;
}

void ChunkPipelineAccumulator::finalize() {
    _finalized = true;
    _mem_usage = 0;
}

void ChunkPipelineAccumulator::reset_state() {
    reset();
    _finalized = false;
}

ChunkPtr& ChunkPipelineAccumulator::pull() {
    if (_finalized && _out_chunk == nullptr) {
        return _in_chunk;
    }
    return _out_chunk;
}

bool ChunkPipelineAccumulator::has_output() const {
    return _out_chunk != nullptr || (_finalized && _in_chunk != nullptr);
}

bool ChunkPipelineAccumulator::need_input() const {
    return !_finalized && _out_chunk == nullptr;
}

bool ChunkPipelineAccumulator::is_finished() const {
    return _finalized && _out_chunk == nullptr && _in_chunk == nullptr;
}

template <class ColumnT>
inline constexpr bool is_object = std::is_same_v<ColumnT, ArrayColumn> || std::is_same_v<ColumnT, StructColumn> ||
                                  std::is_same_v<ColumnT, MapColumn> || std::is_same_v<ColumnT, JsonColumn> ||
                                  std::is_same_v<ObjectColumn<typename ColumnT::ValueType>, ColumnT>;

// Selective-copy data from SegmentedColumn according to provided index
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
        using ColumnT = FixedLengthColumnBase<T>;
        using ContainerT = typename ColumnT::Container;

        _result = column.clone_empty();
        auto output = ColumnHelper::as_column<ColumnT>(_result);

        std::vector<ContainerT*> buffers;
        auto columns = _segment_column->columns();
        for (auto& seg_column : columns) {
            buffers.push_back(&ColumnHelper::as_column<ColumnT>(seg_column)->get_data());
        }

        ContainerT& output_items = output->get_data();
        output_items.resize(_size);
        for (uint32_t i = 0; i < _size; i++) {
            uint32_t idx = _indexes[_from + i];
            auto [segment_id, segment_offset] = _segment_address(idx);
            DCHECK_LT(segment_id, columns.size());
            DCHECK_LT(segment_offset, columns[segment_id]->size());

            output_items[i] = (*buffers[segment_id])[segment_offset];
        }
        return {};
    }

    // Implementation refers to BinaryColumn::append_selective
    template <class Offset>
    Status do_visit(const BinaryColumnBase<Offset>& column) {
        using ColumnT = BinaryColumnBase<Offset>;
        using ContainerT = typename ColumnT::Container*;
        using Bytes = typename ColumnT::Bytes;
        using Byte = typename ColumnT::Byte;
        using Offsets = typename ColumnT::Offsets;

        _result = column.clone_empty();
        auto output = ColumnHelper::as_column<ColumnT>(_result);
        auto& output_offsets = output->get_offset();
        auto& output_bytes = output->get_bytes();

        // input
        auto columns = _segment_column->columns();
        std::vector<Bytes*> input_bytes;
        std::vector<Offsets*> input_offsets;
        for (auto& seg_column : columns) {
            input_bytes.push_back(&ColumnHelper::as_column<ColumnT>(seg_column)->get_bytes());
            input_offsets.push_back(&ColumnHelper::as_column<ColumnT>(seg_column)->get_offset());
        }

#ifndef NDEBUG
        for (auto& src_col : columns) {
            src_col->check_or_die();
        }
#endif

        // assign offsets
        output_offsets.resize(_size + 1);
        size_t num_bytes = 0;
        for (size_t i = 0; i < _size; i++) {
            uint32_t idx = _indexes[_from + i];
            auto [segment_id, segment_offset] = _segment_address(idx);
            DCHECK_LT(segment_id, columns.size());
            DCHECK_LT(segment_offset, columns[segment_id]->size());

            Offsets& src_offsets = *input_offsets[segment_id];
            Offset str_size = src_offsets[segment_offset + 1] - src_offsets[segment_offset];

            output_offsets[i + 1] = output_offsets[i] + str_size;
            num_bytes += str_size;
        }
        output_bytes.resize(num_bytes);

        // copy bytes
        Byte* dest_bytes = output_bytes.data();
        for (size_t i = 0; i < _size; i++) {
            uint32_t idx = _indexes[_from + i];
            auto [segment_id, segment_offset] = _segment_address(idx);
            Bytes& src_bytes = *input_bytes[segment_id];
            Offsets& src_offsets = *input_offsets[segment_id];
            Offset str_size = src_offsets[segment_offset + 1] - src_offsets[segment_offset];
            Byte* str_data = src_bytes.data() + src_offsets[segment_offset];

            strings::memcpy_inlined(dest_bytes + output_offsets[i], str_data, str_size);
        }

#ifndef NDEBUG
        output->check_or_die();
#endif

        return {};
    }

    // Inefficient fallback implementation, it's usually used for Array/Struct/Map/Json
    template <class ColumnT>
    typename std::enable_if_t<is_object<ColumnT>, Status> do_visit(const ColumnT& column) {
        _result = column.clone_empty();
        auto output = ColumnHelper::as_column<ColumnT>(_result);
        output->reserve(_size);

        auto columns = _segment_column->columns();
        for (uint32_t i = 0; i < _size; i++) {
            uint32_t idx = _indexes[_from + i];
            auto [segment_id, segment_offset] = _segment_address(idx);
            output->append(*columns[segment_id], segment_offset, 1);
        }
        return {};
    }

    Status do_visit(const NullableColumn& column) {
        std::vector<ColumnPtr> data_columns, null_columns;
        for (auto& column : _segment_column->columns()) {
            NullableColumn::Ptr nullable = ColumnHelper::as_column<NullableColumn>(column);
            data_columns.push_back(nullable->data_column());
            null_columns.push_back(nullable->null_column());
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

    Status do_visit(const ConstColumn& column) { return Status::NotSupported("SegmentedColumnVisitor"); }

    ColumnPtr result() { return _result; }

private:
    std::pair<int, int> _segment_address(uint32 idx) {
        size_t segment_size = _segment_column->segment_size();
        int segment_id = idx / segment_size;
        int segment_offset = idx % segment_size;
        return {segment_id, segment_offset};
    }

    SegmentedColumnPtr _segment_column;
    ColumnPtr _result;
    const uint32_t* _indexes;
    uint32_t _from;
    uint32_t _size;
};

SegmentedColumn::SegmentedColumn(SegmentedChunkPtr chunk, size_t column_index)
        : _chunk(std::move(chunk)), _column_index(column_index), _segment_size(_chunk->segment_size()) {}

SegmentedColumn::SegmentedColumn(std::vector<ColumnPtr> columns, size_t segment_size)
        : _segment_size(segment_size), _cached_columns(std::move(columns)) {}

ColumnPtr SegmentedColumn::clone_selective(const uint32_t* indexes, uint32_t from, uint32_t size) {
    SegmentedColumnSelectiveCopy visitor(shared_from_this(), indexes, from, size);
    (void)columns()[0]->accept(&visitor);
    return visitor.result();
}

ColumnPtr SegmentedColumn::materialize() const {
    auto actual_columns = columns();
    if (actual_columns.empty()) {
        return {};
    }
    ColumnPtr result = actual_columns[0]->clone_empty();
    for (size_t i = 0; i < actual_columns.size(); i++) {
        result->append(*actual_columns[i]);
    }
    return result;
}

size_t SegmentedColumn::segment_size() const {
    return _segment_size;
}

size_t SegmentedChunk::segment_size() const {
    return _segment_size;
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

std::vector<ColumnPtr> SegmentedColumn::columns() const {
    if (!_cached_columns.empty()) {
        return _cached_columns;
    }
    std::vector<ColumnPtr> columns;
    for (auto& segment : _chunk->segments()) {
        columns.push_back(segment->get_column_by_index(_column_index));
    }
    return columns;
}

void SegmentedColumn::upgrade_to_nullable() {
    for (auto& segment : _chunk->segments()) {
        auto& column = segment->get_column_by_index(_column_index);
        column = NullableColumn::wrap_if_necessary(column);
    }
}

SegmentedChunk::SegmentedChunk(size_t segment_size) : _segment_size(segment_size) {
    // put at least one chunk there
    _segments.resize(1);
    _segments[0] = std::make_shared<Chunk>();
}

SegmentedChunkPtr SegmentedChunk::create(size_t segment_size) {
    return std::make_shared<SegmentedChunk>(segment_size);
}

void SegmentedChunk::append_column(ColumnPtr column, SlotId slot_id) {
    // It's only used when initializing the chunk, so append the column to first chunk is enough
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
            open_segment->columns()[i]->append(*column, append_index, open_segment_append_rows);
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
        // The segment need to cutoff
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

} // namespace starrocks