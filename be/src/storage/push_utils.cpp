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

#include "storage/push_utils.h"

namespace starrocks {

PushBrokerReader::~PushBrokerReader() {
    _counter.reset();
    _scanner.reset();
}

Status PushBrokerReader::init(const TBrokerScanRange& t_scan_range, const TPushReq& request) {
    // init runtime state, runtime profile, counter
    TUniqueId dummy_id;
    dummy_id.hi = 0;
    dummy_id.lo = 0;
    TPlanFragmentExecParams params;
    params.fragment_instance_id = dummy_id;
    params.query_id = dummy_id;
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = InternalServiceVersion::V1;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    if (request.__isset.timezone) {
        query_globals.__set_time_zone(request.timezone);
    }
    _runtime_state =
            std::make_unique<RuntimeState>(fragment_params.params.query_id, fragment_params.params.fragment_instance_id,
                                           query_options, query_globals, ExecEnv::GetInstance());

    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state.get(), _runtime_state->obj_pool(), request.desc_tbl, &desc_tbl,
                                          config::vector_chunk_size));
    _runtime_state->set_desc_tbl(desc_tbl);

    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("PushBrokerReader");

    _runtime_state->init_mem_trackers(dummy_id);

    // init tuple desc
    auto tuple_id = t_scan_range.params.dest_tuple_id;
    _tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError(strings::Substitute("Failed to get tuple descriptor, tuple_id: $0", tuple_id));
    }

    // init counter
    _counter = std::make_unique<ScannerCounter>();

    // init scanner
    FileScanner* scanner = nullptr;
    switch (t_scan_range.ranges[0].format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        scanner = new ParquetScanner(_runtime_state.get(), _runtime_profile, t_scan_range, _counter.get());
        if (scanner == nullptr) {
            return Status::InternalError("Failed to create scanner");
        }
        break;
    }
    default:
        return Status::NotSupported(
                strings::Substitute("Unsupported file format type: $0", t_scan_range.ranges[0].format_type));
    }
    _scanner.reset(scanner);
    RETURN_IF_ERROR(_scanner->open());

    _ready = true;
    return Status::OK();
}

ColumnPtr PushBrokerReader::_build_object_column(const ColumnPtr& column) {
    ColumnBuilder<TYPE_OBJECT> builder(config::vector_chunk_size);
    ColumnViewer<TYPE_VARCHAR> viewer(column);

    if (!column->has_null()) {
        for (int row = 0; row < viewer.size(); ++row) {
            auto value = viewer.value(row);
            BitmapValue bitmap(value.data);
            builder.append(&bitmap);
        }
    } else {
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            auto value = viewer.value(row);
            BitmapValue bitmap(value.data);
            builder.append(&bitmap);
        }
    }

    return builder.build(false);
}

ColumnPtr PushBrokerReader::_build_hll_column(const ColumnPtr& column) {
    ColumnBuilder<TYPE_HLL> builder(config::vector_chunk_size);
    ColumnViewer<TYPE_VARCHAR> viewer(column);

    if (!column->has_null()) {
        for (int row = 0; row < viewer.size(); ++row) {
            auto value = viewer.value(row);
            HyperLogLog hll(Slice(value.data, value.size));
            builder.append(&hll);
        }
    } else {
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            auto value = viewer.value(row);
            HyperLogLog hll(Slice(value.data, value.size));
            builder.append(&hll);
        }
    }

    return builder.build(false);
}

ColumnPtr PushBrokerReader::_padding_char_column(const ColumnPtr& column, const SlotDescriptor* slot_desc,
                                                 size_t num_rows) {
    Column* data_column = ColumnHelper::get_data_column(column.get());
    auto* binary = down_cast<BinaryColumn*>(data_column);
    Offsets& offset = binary->get_offset();
    uint32_t len = slot_desc->type().len;

    // Padding 0 to CHAR field, the storage bitmap index and zone map need it.
    auto new_binary = BinaryColumn::create();
    Offsets& new_offset = new_binary->get_offset();
    Bytes& new_bytes = new_binary->get_bytes();
    new_offset.resize(num_rows + 1);
    new_bytes.assign(num_rows * len, 0); // padding 0

    uint32_t from = 0;
    Bytes& bytes = binary->get_bytes();
    for (size_t i = 0; i < num_rows; ++i) {
        uint32_t copy_data_len = std::min(len, offset[i + 1] - offset[i]);
        strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset[i], copy_data_len);
        from += len; // no copy data will be 0
    }

    for (size_t i = 1; i <= num_rows; ++i) {
        new_offset[i] = len * i;
    }

    if (slot_desc->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column.get());
        return NullableColumn::create(new_binary, nullable_column->null_column());
    }
    return new_binary;
}

Status PushBrokerReader::_convert_chunk(const ChunkPtr& from, ChunkPtr* to) {
    DCHECK(from->num_columns() == (*to)->num_columns()) << "Chunk schema is different";
    DCHECK(from->num_columns() == _tuple_desc->slots().size()) << "Tuple slot size and chunk schema is different";

    size_t num_rows = from->num_rows();
    for (int i = 0; i < from->num_columns(); ++i) {
        auto from_col = from->get_column_by_index(i);
        auto to_col = (*to)->get_column_by_index(i);

        const SlotDescriptor* slot_desc = _tuple_desc->slots().at(i);
        const TypeDescriptor& type_desc = slot_desc->type();
        from_col = ColumnHelper::unfold_const_column(type_desc, num_rows, from_col);

        switch (type_desc.type) {
        case TYPE_OBJECT:
            // deserialize bitmap column from varchar that read from parquet file.
            from_col = _build_object_column(from_col);
            break;
        case TYPE_HLL:
            // deserialize hll column from varchar that read from parquet file.
            from_col = _build_hll_column(from_col);
            break;
        case TYPE_CHAR:
            from_col = _padding_char_column(from_col, slot_desc, num_rows);
            break;
        default:
            break;
        }

        // column is nullable or not should be determined by schema.
        if (slot_desc->is_nullable()) {
            to_col->append(*from_col);
        } else {
            // not null
            if (from_col->is_nullable() && from_col->has_null()) {
                return Status::InternalError(
                        strings::Substitute("non-nullable column has null data, name: $0", to_col->get_name()));
            }

            auto data_col = ColumnHelper::get_data_column(from_col.get());
            to_col->append(*data_col);
        }
    }

    return Status::OK();
}

Status PushBrokerReader::next_chunk(ChunkPtr* chunk) {
    if (!_ready) {
        return Status::InternalError("Not ready");
    }

    auto res = _scanner->get_next();
    if (res.status().is_end_of_file()) {
        _eof = true;
        return Status::OK();
    } else if (!res.ok()) {
        return res.status();
    }

    return _convert_chunk(res.value(), chunk);
}

void PushBrokerReader::print_profile() {
    std::stringstream ss;
    _runtime_profile->pretty_print(&ss);
    LOG(INFO) << ss.str();
}

} // namespace starrocks
