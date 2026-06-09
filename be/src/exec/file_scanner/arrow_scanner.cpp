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

#include "exec/file_scanner/arrow_scanner.h"

#include "base/simd/simd.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr_factory.h"
#include "fs/fs_broker.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "base/time/timezone_utils.h"

namespace starrocks {

ArrowScanner::ArrowScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                           ScannerCounter* counter, bool schema_only)
        : FileScanner(state, profile, scan_range.params, counter, schema_only),
          _scan_range(scan_range),
          _curr_file_reader(nullptr),
          _max_chunk_size(state->chunk_size() ? state->chunk_size() : 4096) {
    _file_format_str = "arrow";
    _chunk_filter.reserve(_max_chunk_size);
    _conv_ctx.timezone = state->timezone();
}

ArrowScanner::~ArrowScanner() = default;

Status ArrowScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
    auto range = _scan_range.ranges[0];
    _num_of_columns_from_file = range.__isset.num_of_columns_from_file
                                        ? implicit_cast<int>(range.num_of_columns_from_file)
                                        : implicit_cast<int>(_src_slot_descriptors.size());

    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
    _cast_exprs.resize(_num_of_columns_from_file, nullptr);

    if (range.__isset.num_of_columns_from_file) {
        int nums = range.columns_from_path.size();
        for (const auto& rng : _scan_range.ranges) {
            if (nums != rng.columns_from_path.size()) {
                return Status::InternalError("Different range different columns.");
            }
        }
    }
    return Status::OK();
}

Status ArrowScanner::open_next_reader() {
    if (_next_file >= _scan_range.ranges.size()) {
        _scanner_eof = true;
        return Status::EndOfFile("no more files to read");
    }
    const auto& range_desc = _scan_range.ranges[_next_file++];
    
    std::shared_ptr<SequentialFile> file;
    RETURN_IF_ERROR(create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &file));
    
    class StarRocksArrowInputStream : public arrow::io::InputStream {
    public:
        StarRocksArrowInputStream(std::shared_ptr<SequentialFile> file) : _file(std::move(file)), _pos(0) {}
        
        ~StarRocksArrowInputStream() override = default;
        
        arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
            auto res = _file->read(out, nbytes);
            if (!res.ok()) {
                return arrow::Status::IOError(res.status().to_string());
            }
            _pos += res.value();
            return res.value();
        }
        
        arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
            ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes));
            ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buf->mutable_data()));
            if (bytes_read < nbytes) {
                ARROW_RETURN_NOT_OK(buf->Resize(bytes_read));
            }
            return std::shared_ptr<arrow::Buffer>(std::move(buf));
        }
        
        arrow::Result<int64_t> Tell() const override {
            return _pos;
        }
        
        arrow::Status Close() override {
            return arrow::Status::OK();
        }
        
        bool closed() const override {
            return false;
        }
        
    private:
        std::shared_ptr<SequentialFile> _file;
        int64_t _pos;
    };
    
    auto arrow_stream = std::make_shared<StarRocksArrowInputStream>(std::move(file));
    auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(arrow_stream);
    if (!reader_res.ok()) {
        return Status::InternalError("open RecordBatchStreamReader failed, reason: " + reader_res.status().ToString());
    }
    
    _curr_file_reader = reader_res.ValueOrDie();
    _conv_ctx.current_file = range_desc.path;
    return Status::OK();
}

Status ArrowScanner::next_batch() {
    _batch_start_idx = 0;
    _batch = nullptr;
    if (_curr_file_reader == nullptr) {
        auto status = open_next_reader();
        if (!status.ok()) {
            if (status.is_end_of_file()) {
                _scanner_eof = true;
            }
            return status;
        }
    }
    
    arrow::Status status = _curr_file_reader->ReadNext(&_batch);
    if (!status.ok()) {
        return Status::InternalError("ReadNext batch failed, reason: " + status.ToString());
    }
    
    if (_batch == nullptr) {
        _curr_file_reader.reset();
        return Status::EndOfFile("reach end of current file");
    }
    
    return Status::OK();
}

Status ArrowScanner::initialize_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    _pool.clear();
    (*chunk) = std::make_shared<Chunk>();
    _chunk_filter.clear();
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        MutableColumnPtr column;
        auto array_ptr = _batch->GetColumnByName(std::string(slot_desc->col_name()));
        if (array_ptr == nullptr) {
            _cast_exprs[i] = _pool.add(new ColumnRef(slot_desc));
            column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        } else {
            RETURN_IF_ERROR(new_column(array_ptr->type().get(), slot_desc, &column, _conv_funcs[i].get(),
                                       &_cast_exprs[i], _pool, _strict_mode));
        }
        column->reserve(_max_chunk_size);
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }
    return Status::OK();
}

Status ArrowScanner::append_batch_to_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->fill_ns);
    size_t num_elements =
            std::min<size_t>((_max_chunk_size - _chunk_start_idx), (_batch->num_rows() - _batch_start_idx));
    _chunk_filter.resize(_chunk_filter.size() + num_elements, 1);
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.set_current_column(slot_desc->col_name(), slot_desc->type());
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_desc->id());
        auto array_ptr = _batch->GetColumnByName(std::string(slot_desc->col_name()));
        if (array_ptr == nullptr) {
            (void)column->append_nulls(num_elements);
        } else {
            auto st = convert_arrow_array_to_column(_conv_funcs[i].get(), num_elements, array_ptr.get(), column,
                                                    _batch_start_idx, _chunk_start_idx, &_chunk_filter, &_conv_ctx);
            if (!st.ok()) {
                return st.clone_and_prepend(strings::Substitute("file=$0 column=$1 batch_row_range=[$2,$3)",
                                                                _conv_ctx.current_file, slot_desc->col_name(),
                                                                _batch_start_idx, _batch_start_idx + num_elements));
            }
        }
    }

    _chunk_start_idx += num_elements;
    _batch_start_idx += num_elements;
    return Status::OK();
}

Status ArrowScanner::finalize_src_chunk(ChunkPtr* chunk) {
    auto num_rows = (*chunk)->filter(_chunk_filter);
    _counter->num_rows_filtered += _chunk_start_idx - num_rows;
    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    {
        SCOPED_RAW_TIMER(&_counter->cast_chunk_ns);
        for (auto i = 0; i < _num_of_columns_from_file; ++i) {
            SlotDescriptor* slot_desc = _src_slot_descriptors[i];
            if (slot_desc == nullptr) {
                continue;
            }

            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, (*chunk).get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), (*chunk)->num_rows(), column);
            cast_chunk->append_column(column, slot_desc->id());
        }
        auto range = _scan_range.ranges.at(_next_file - 1);
        if (range.__isset.num_of_columns_from_file) {
            fill_columns_from_path(cast_chunk, range.num_of_columns_from_file, range.columns_from_path,
                                   cast_chunk->num_rows());
        }
    }
    ASSIGN_OR_RETURN(auto dest_chunk, materialize(*chunk, cast_chunk));
    *chunk = dest_chunk;
    _chunk_start_idx = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> ArrowScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    ChunkPtr chunk;
    if (batch_is_exhausted()) {
        while (true) {
            Status status = next_batch();
            if (_scanner_eof) {
                return status;
            }
            if (status.ok()) {
                break;
            }
            if (status.is_end_of_file()) {
                _curr_file_reader.reset();
                continue;
            }
            return status;
        }
    }
    RETURN_IF_ERROR(initialize_src_chunk(&chunk));
    while (!_scanner_eof) {
        RETURN_IF_ERROR(append_batch_to_src_chunk(&chunk));
        if (chunk_is_full()) {
            break;
        }
        auto status = next_batch();
        if (status.ok()) {
            continue;
        }
        if (!status.is_end_of_file()) {
            return status;
        }

        _curr_file_reader.reset();
        if (chunk->num_rows() > 0) {
            break;
        }
        RETURN_IF_ERROR(next_batch());
        RETURN_IF_ERROR(initialize_src_chunk(&chunk));
    }
    RETURN_IF_ERROR(finalize_src_chunk(&chunk));
    return std::move(chunk);
}

Status ArrowScanner::get_schema(std::vector<SlotDescriptor>* schema) {
    return Status::NotSupported("Arrow schema inference not supported yet");
}

void ArrowScanner::close() {
    FileScanner::close();
    _curr_file_reader.reset();
    _pool.clear();
}

Status ArrowScanner::new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc,
                                MutableColumnPtr* column, ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool,
                                bool strict_mode) {
    auto& type_desc = slot_desc->type();
    TypeDescriptor raw_type_desc;
    bool need_cast = false;
    RETURN_IF_ERROR(build_arrow_column_convert_plan(arrow_type, &type_desc, slot_desc->is_nullable(), &raw_type_desc,
                                                    conv_func, need_cast, strict_mode));
    *column = create_arrow_column_convert_dest(type_desc, raw_type_desc, need_cast, slot_desc->is_nullable());
    if (!need_cast) {
        *expr = pool.add(new ColumnRef(slot_desc));
    } else {
        auto slot = pool.add(new ColumnRef(slot_desc));
        *expr = VectorizedCastExprFactory::from_type(raw_type_desc, slot_desc->type(), slot, &pool);
        if ((*expr) == nullptr) {
            return illegal_converting_error(arrow_type->name(), type_desc.debug_string());
        }
    }
    return Status::OK();
}

bool ArrowScanner::chunk_is_full() {
    return _chunk_start_idx >= _max_chunk_size;
}

bool ArrowScanner::batch_is_exhausted() {
    return _scanner_eof || _batch == nullptr || _batch_start_idx >= _batch->num_rows();
}

} // namespace starrocks
