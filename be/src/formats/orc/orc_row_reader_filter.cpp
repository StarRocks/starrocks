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

#include "formats/orc/orc_row_reader_filter.h"

namespace starrocks {

OrcRowReaderFilter::OrcRowReaderFilter(const HdfsScannerParams& scanner_params, const HdfsScannerContext& scanner_ctx,
                                       OrcChunkReader* reader)
        : _scanner_params(scanner_params),
          _scanner_ctx(scanner_ctx),

          _reader(reader),
          _writer_tzoffset_in_seconds(reader->tzoffset_in_seconds()) {
    if (_scanner_params.min_max_tuple_desc != nullptr) {
        VLOG_FILE << "OrcRowReaderFilter: min_max_tuple_desc = " << _scanner_params.min_max_tuple_desc->debug_string();
        for (ExprContext* ctx : _scanner_params.min_max_conjunct_ctxs) {
            VLOG_FILE << "OrcRowReaderFilter: min_max_ctx = " << ctx->root()->debug_string();
        }
    }
    for (const auto& r : _scanner_params.scan_ranges) {
        _scan_ranges.insert(std::make_pair(r->offset + r->length, r->offset));
    }
}

void OrcRowReaderFilter::onStartingPickRowGroups() {}

void OrcRowReaderFilter::onEndingPickRowGroups() {}

void OrcRowReaderFilter::setWriterTimezone(const std::string& tz) {
    cctz::time_zone writer_tzinfo;
    if (tz == "" || !TimezoneUtils::find_cctz_time_zone(tz, writer_tzinfo)) {
        _writer_tzoffset_in_seconds = _reader->tzoffset_in_seconds();
    } else {
        _writer_tzoffset_in_seconds = TimezoneUtils::to_utc_offset(writer_tzinfo);
    }
}

bool OrcRowReaderFilter::filterOnOpeningStripe(uint64_t stripeIndex,
                                               const orc::proto::StripeInformation* stripeInformation) {
    _current_stripe_index = stripeIndex;
    uint64_t offset = stripeInformation->offset();
    // range end must > offset
    auto it = _scan_ranges.upper_bound(offset);
    if ((it != _scan_ranges.end()) && (offset >= it->second) && (offset < it->first)) {
        return false;
    }
    return true;
}

bool OrcRowReaderFilter::filterMinMax(size_t rowGroupIdx,
                                      const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                                      const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilter) {
    const TupleDescriptor* min_max_tuple_desc = _scanner_params.min_max_tuple_desc;
    ChunkPtr min_chunk = ChunkHelper::new_chunk(*min_max_tuple_desc, 0);
    ChunkPtr max_chunk = ChunkHelper::new_chunk(*min_max_tuple_desc, 0);
    for (size_t i = 0; i < min_max_tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = min_max_tuple_desc->slots()[i];
        int32_t column_index = _reader->get_column_id_by_name(slot->col_name());
        if (column_index >= 0) {
            auto row_idx_iter = rowIndexes.find(column_index);
            // there is no column stats, skip filter process.
            if (row_idx_iter == rowIndexes.end()) {
                return false;
            }
            const orc::proto::ColumnStatistics& stats = row_idx_iter->second.entry(rowGroupIdx).statistics();
            ColumnPtr min_col = min_chunk->columns()[i];
            ColumnPtr max_col = max_chunk->columns()[i];
            DCHECK(!min_col->is_constant() && !max_col->is_constant());
            int64_t tz_offset_in_seconds = _reader->tzoffset_in_seconds() - _writer_tzoffset_in_seconds;
            Status st = OrcMinMaxDecoder::decode(slot, stats, min_col, max_col, tz_offset_in_seconds);
            if (!st.ok()) {
                return false;
            }
        } else {
            // search partition columns.
            int part_idx = 0;
            const int part_size = _scanner_ctx.partition_columns.size();
            for (part_idx = 0; part_idx < part_size; part_idx++) {
                if (_scanner_ctx.partition_columns[part_idx].col_name == slot->col_name()) {
                    break;
                }
            }
            // not found in partition columns.
            if (part_idx == part_size) {
                min_chunk->columns()[i]->append_nulls(1);
                max_chunk->columns()[i]->append_nulls(1);
            } else {
                auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(_scanner_ctx.partition_values[part_idx]);
                min_chunk->columns()[i]->append(*const_column->data_column(), 0, 1);
                max_chunk->columns()[i]->append(*const_column->data_column(), 0, 1);
            }
        }
    }

    VLOG_FILE << "stripe = " << _current_stripe_index << ", row_group = " << rowGroupIdx
              << ", min_chunk = " << min_chunk->debug_row(0) << ", max_chunk = " << max_chunk->debug_row(0);
    for (auto& min_max_conjunct_ctx : _scanner_ctx.min_max_conjunct_ctxs) {
        // TODO: add a warning log here
        auto min_col = EVALUATE_NULL_IF_ERROR(min_max_conjunct_ctx, min_max_conjunct_ctx->root(), min_chunk.get());
        auto max_col = EVALUATE_NULL_IF_ERROR(min_max_conjunct_ctx, min_max_conjunct_ctx->root(), max_chunk.get());
        auto min = min_col->get(0).get_int8();
        auto max = max_col->get(0).get_int8();
        if (min == 0 && max == 0) {
            return true;
        }
    }
    return false;
}
bool OrcRowReaderFilter::filterOnPickRowGroup(size_t rowGroupIdx,
                                              const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) {
    if (_scanner_params.min_max_tuple_desc != nullptr) {
        if (filterMinMax(rowGroupIdx, rowIndexes, bloomFilters)) {
            VLOG_FILE << "OrcRowReaderFilter: skip row group " << rowGroupIdx << ", stripe " << _current_stripe_index;
            return true;
        }
    }
    return false;
}

bool OrcRowReaderFilter::filterOnPickStringDictionary(
        const std::unordered_map<uint64_t, orc::StringDictionary*>& sdicts) {
    if (sdicts.empty()) return false;

    if (!_init_use_dict_filter_slots) {
        for (auto& col : _scanner_ctx.materialized_columns) {
            SlotDescriptor* slot = col.slot_desc;
            if (!_scanner_ctx.can_use_dict_filter_on_slot(slot)) {
                continue;
            }
            int32_t column_index = _reader->get_column_id_by_name(col.col_name);
            if (column_index < 0) {
                continue;
            }
            _use_dict_filter_slots.emplace_back(std::make_pair(slot, column_index));
        }
        _init_use_dict_filter_slots = true;
    }

    _dict_filter_eval_cache.clear();

    for (auto& p : _use_dict_filter_slots) {
        SlotDescriptor* slot_desc = p.first;
        SlotId slot_id = slot_desc->id();
        uint64_t column_index = p.second;
        const auto& it = sdicts.find(column_index);
        if (it == sdicts.end()) {
            continue;
        }
        // create chunk
        orc::StringDictionary* dict = it->second;
        if (dict->dictionaryOffset.size() > _reader->read_chunk_size()) {
            continue;
        }
        ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
        // always assume there is a possibility of null value in ORC column.
        // and we evaluate with null always.
        ColumnPtr column_ptr = ColumnHelper::create_column(slot_desc->type(), true);
        dict_value_chunk->append_column(column_ptr, slot_id);

        auto* nullable_column = down_cast<NullableColumn*>(column_ptr.get());
        auto* dict_value_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());

        // copy dict and offset to column.
        Bytes& bytes = dict_value_column->get_bytes();
        Offsets& offsets = dict_value_column->get_offset();

        const char* content_data = dict->dictionaryBlob.data();
        size_t content_size = dict->dictionaryBlob.size();
        bytes.reserve(content_size);
        const auto* start = reinterpret_cast<const uint8_t*>(content_data);
        const auto* end = reinterpret_cast<const uint8_t*>(content_data + content_size);

        size_t offset_size = dict->dictionaryOffset.size();
        size_t dict_size = offset_size - 1;
        const int64_t* offset_data = dict->dictionaryOffset.data();
        offsets.resize(offset_size);

        if (slot_desc->type().type == TYPE_CHAR) {
            // for char type, dict strings are also padded with spaces.
            // we also have to strip space off. For example
            // | hello      |  world      | yes     |, we have to compact to
            // | hello | world | yes |
            size_t total_size = 0;
            const char* p_start = reinterpret_cast<const char*>(start);
            for (size_t i = 0; i < dict_size; i++) {
                const char* s = p_start + offset_data[i];
                size_t old_size = offset_data[i + 1] - offset_data[i];
                size_t new_size = remove_trailing_spaces(s, old_size);
                bytes.insert(bytes.end(), s, s + new_size);
                offsets[i] = total_size;
                total_size += new_size;
            }
            offsets[dict_size] = total_size;
        } else {
            bytes.insert(bytes.end(), start, end);
            // type mismatch, have to use loop to assign.
            for (size_t i = 0; i < offset_size; i++) {
                offsets[i] = offset_data[i];
            }
        }

        // first (dict_size) th items are all not-null
        nullable_column->null_column()->append_default(dict_size);
        // and last one is null.
        nullable_column->append_default();
        DCHECK(nullable_column->size() == (dict_size + 1));

        VLOG_FILE << "OrcRowReaderFilter: stripe = " << _current_stripe_index
                  << ", slot = " << slot_desc->debug_string()
                  << ", dict values = " << dict_value_column->debug_string();

        FilterPtr* filter_ptr = nullptr;
        if (_can_do_filter_on_orc_cvb) {
            // not sure it's best practice.
            FilterPtr tmp;
            _dict_filter_eval_cache[slot_id] = tmp;
            filter_ptr = &_dict_filter_eval_cache[slot_id];
        }

        // do evaluation with dictionary.
        ExecNode::eval_conjuncts(_scanner_ctx.conjunct_ctxs_by_slot.at(slot_id), dict_value_chunk.get(), filter_ptr);
        if (dict_value_chunk->num_rows() == 0) {
            // release memory early.
            _dict_filter_eval_cache.clear();
            VLOG_FILE << "OrcRowReaderFilter: skip stripe by dict filter, stripe " << _current_stripe_index
                      << ", on slot = " << slot_desc->debug_string();
            return true;
        }
        DCHECK(filter_ptr->get() != nullptr);
    }

    return false;
}
} // namespace starrocks