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

#include "exec/orc_scanner.h"

#include <memory>

#include "column/array_column.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/broker_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ORCFileStream : public ORCHdfsFileStream {
public:
    ORCFileStream(std::shared_ptr<RandomAccessFile> file, uint64_t length, starrocks::ScannerCounter* counter)
            : ORCHdfsFileStream(file.get(), length, nullptr), _file(std::move(file)), _counter(counter) {}

    ~ORCFileStream() override { _file.reset(); }

    void read(void* buf, uint64_t length, uint64_t offset) override {
        SCOPED_RAW_TIMER(&_counter->file_read_ns);
        ORCHdfsFileStream::read(buf, length, offset);
    }

private:
    std::shared_ptr<RandomAccessFile> _file;
    ScannerCounter* _counter;
};

ORCScanner::ORCScanner(starrocks::RuntimeState* state, starrocks::RuntimeProfile* profile,
                       const TBrokerScanRange& scan_range, starrocks::ScannerCounter* counter, bool schema_only)
        : FileScanner(state, profile, scan_range.params, counter, schema_only),
          _scan_range(scan_range),
          _max_chunk_size(_state->chunk_size() ? _state->chunk_size() : 4096),
          _next_range(0),
          _error_counter(0),
          _status_eof(false) {}

Status ORCScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        _status_eof = true;
        return Status::OK();
    }

    auto range = _scan_range.ranges[0];
    int num_columns_from_orc = range.__isset.num_of_columns_from_file
                                       ? implicit_cast<int>(range.num_of_columns_from_file)
                                       : implicit_cast<int>(_src_slot_descriptors.size());

    // column from path
    if (range.__isset.num_of_columns_from_file) {
        int nums = range.columns_from_path.size();
        for (const auto& rng : _scan_range.ranges) {
            if (nums != rng.columns_from_path.size()) {
                return Status::InternalError("Different range different columns.");
            }
        }
    }

    // just slot descriptors that's going to read from orc file.
    _orc_slot_descriptors.resize(num_columns_from_orc);
    for (int i = 0; i < num_columns_from_orc; ++i) {
        _orc_slot_descriptors[i] = _src_slot_descriptors[i];
    }
    _orc_reader = std::make_unique<OrcChunkReader>(_state->chunk_size(), _orc_slot_descriptors);
    _orc_reader->set_broker_load_mode(_strict_mode);
    RETURN_IF_ERROR(_orc_reader->set_timezone(_state->timezone()));
    _orc_reader->drop_nanoseconds_in_datetime();
    _orc_reader->set_runtime_state(_state);
    _orc_reader->set_case_sensitive(_case_sensitive);
    RETURN_IF_ERROR(_open_next_orc_reader());

    return Status::OK();
}

Status ORCScanner::get_schema(std::vector<SlotDescriptor>* schema) {
    return _orc_reader->get_schema(schema);
}

StatusOr<ChunkPtr> ORCScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    if (_status_eof) {
        return Status::EndOfFile("eof");
    }

    ChunkPtr tmp_chunk = nullptr;
    while (true) {
        auto result = _next_orc_chunk();
        if (!result.ok()) {
            return result;
        }
        tmp_chunk = std::move(result.value());
        if (!tmp_chunk->is_empty()) {
            break;
        }
    }
    ASSIGN_OR_RETURN(auto cast_chunk, _transfer_chunk(tmp_chunk));
    // use base class implementation. they are the SAME!!!
    return materialize(tmp_chunk, cast_chunk);
}

StatusOr<ChunkPtr> ORCScanner::_next_orc_chunk() {
    try {
        ChunkPtr chunk = _create_src_chunk();
        RETURN_IF_ERROR(_next_orc_batch(&chunk));
        // fill path column
        const TBrokerRangeDesc& range = _scan_range.ranges.at(_next_range - 1);
        if (range.__isset.num_of_columns_from_file) {
            fill_columns_from_path(chunk, range.num_of_columns_from_file, range.columns_from_path, chunk->num_rows());
        }
        return std::move(chunk);
    } catch (orc::ParseError& e) {
        std::string s = strings::Substitute("ParseError: $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    } catch (orc::InvalidArgument& e) {
        std::string s = strings::Substitute("ParseError: $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }
    return Status::InternalError("unreachable path");
}

StatusOr<ChunkPtr> ORCScanner::_transfer_chunk(starrocks::ChunkPtr& src) {
    SCOPED_RAW_TIMER(&_counter->cast_chunk_ns);
    ASSIGN_OR_RETURN(ChunkPtr cast_chunk, _orc_reader->cast_chunk_checked(&src));
    auto range = _scan_range.ranges.at(_next_range - 1);
    if (range.__isset.num_of_columns_from_file) {
        for (int i = 0; i < range.columns_from_path.size(); ++i) {
            auto slot = _src_slot_descriptors[range.num_of_columns_from_file + i];
            // This happens when there are extra fields in broker load specification
            // but those extra fields don't match any fields in native table.
            if (slot != nullptr) {
                cast_chunk->append_column(src->get_column_by_slot_id(slot->id()), slot->id());
            }
        }
    }
    return cast_chunk;
}

ChunkPtr ORCScanner::_create_src_chunk() {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    ChunkPtr chunk = _orc_reader->create_chunk();
    return chunk;
}

Status ORCScanner::_next_orc_batch(ChunkPtr* result) {
    {
        SCOPED_RAW_TIMER(&_counter->read_batch_ns);
        Status status = _orc_reader->read_next();
        while (status.is_end_of_file()) {
            RETURN_IF_ERROR(_open_next_orc_reader());
            status = _orc_reader->read_next();
            if (status.is_end_of_file()) {
                continue;
            }
            RETURN_IF_ERROR(status);
        }
        RETURN_IF_ERROR(status);
    }
    {
        SCOPED_RAW_TIMER(&_counter->fill_ns);
        RETURN_IF_ERROR(_orc_reader->fill_chunk(result));
        _counter->num_rows_filtered += _orc_reader->get_num_rows_filtered();
    }
    return Status::OK();
}

Status ORCScanner::_open_next_orc_reader() {
    while (true) {
        _state->update_num_bytes_scan_from_source(_last_file_size);
        if (_next_range >= _scan_range.ranges.size()) {
            return Status::EndOfFile("no more file to be read");
        }
        std::shared_ptr<RandomAccessFile> file;
        const TBrokerRangeDesc& range_desc = _scan_range.ranges[_next_range];
        Status st = create_random_access_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params,
                                              CompressionTypePB::NO_COMPRESSION, &file);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to create random-access files. status: " << st.to_string();
            return st;
        }
        const std::string& file_name = file->filename();
        ASSIGN_OR_RETURN(uint64_t file_size, file->get_size());
        auto inStream = std::make_unique<ORCFileStream>(file, file_size, _counter);
        _next_range++;
        _last_file_size = file_size;
        _orc_reader->set_read_chunk_size(_max_chunk_size);
        _orc_reader->set_current_file_name(file_name);
        st = _orc_reader->init(std::move(inStream));
        if (st.is_end_of_file()) {
            LOG(WARNING) << "Failed to init orc reader. filename: " << file_name << ", status: " << st.to_string();
            continue;
        }
        return st;
    }
}

void ORCScanner::close() {
    FileScanner::close();
    _orc_reader.reset(nullptr);
}

} // namespace starrocks
