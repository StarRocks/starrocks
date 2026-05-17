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

#pragma once

#include <avrocpp/DataFile.hh>
#include <avrocpp/Generic.hh>
#include <avrocpp/Stream.hh>

#include "column/vectorized_fwd.h"
#include "formats/avro/cpp/column_reader.h"

namespace starrocks {

struct ScannerCounter;
class AdaptiveNullableColumn;
class RandomAccessFile;
class RuntimeState;
class SlotDescriptor;

class AvroBufferInputStream final : public avro::SeekableInputStream {
public:
    // Owning: takes a shared_ptr and keeps it alive for the stream's lifetime.
    AvroBufferInputStream(std::shared_ptr<RandomAccessFile> file, size_t buffer_size, ScannerCounter* counter)
            : _file_owned(std::move(file)),
              _file(_file_owned.get()),
              _buffer_size(buffer_size),
              _buffer(new uint8_t[buffer_size]),
              _next(_buffer),
              _counter(counter) {}

    // Non-owning: caller guarantees the file outlives this stream.
    AvroBufferInputStream(RandomAccessFile* file, size_t buffer_size, ScannerCounter* counter)
            : _file(file),
              _buffer_size(buffer_size),
              _buffer(new uint8_t[buffer_size]),
              _next(_buffer),
              _counter(counter) {}

    ~AvroBufferInputStream() override { delete[] _buffer; }

    bool next(const uint8_t** data, size_t* len) override;
    void backup(size_t len) override;
    void skip(size_t len) override;
    size_t byteCount() const override { return _byte_count; }
    void seek(int64_t position) override;

private:
    bool fill();

    std::shared_ptr<RandomAccessFile> _file_owned; // non-null only for the owning constructor
    RandomAccessFile* _file = nullptr;             // always valid; either _file_owned.get() or caller-managed
    const size_t _buffer_size;
    uint8_t* const _buffer;
    size_t _byte_count{0};
    uint8_t* _next;
    size_t _available{0};
    ScannerCounter* _counter = nullptr;
};

class AvroReader {
public:
    AvroReader() = default;
    ~AvroReader();

    // raw_file and buffer_size are optional; when provided and no columns are needed
    // (count(*) path), records are counted by reading only Avro block headers — no
    // decompression — which is much faster than record-level decoding.
    //
    // split_offset / split_length: when non-zero the reader operates in split mode.
    //   - init() calls DataFileReader::sync(split_offset), which advances to the first
    //     sync marker at or after split_offset.  This is the same strategy used by
    //     Hadoop's AvroInputFormat.
    //   - read_chunk() stops once DataFileReader::pastSync(split_offset + split_length)
    //     returns true, i.e. once the reader has passed the end of the split.
    // When split_offset == 0 the reader starts from the beginning of the file (first
    // split or unsplit scan), and split_length == 0 means "read to end of file".
    Status init(std::unique_ptr<avro::InputStream> input_stream, const std::string& filename, RuntimeState* state,
                ScannerCounter* counter, const std::vector<SlotDescriptor*>* slot_descs,
                const std::vector<avrocpp::ColumnReaderUniquePtr>* column_readers, bool col_not_found_as_null,
                RandomAccessFile* raw_file = nullptr, size_t buffer_size = 0, int64_t split_offset = 0,
                int64_t split_length = 0, const std::string& reader_schema_json = "");

    void TEST_init(const std::vector<SlotDescriptor*>* slot_descs,
                   const std::vector<avrocpp::ColumnReaderUniquePtr>* column_readers, bool col_not_found_as_null);

    // rows_counted_out: when non-null, receives the number of Avro records traversed
    // in the no-materialized-column path (used by do_get_next for count queries).
    Status read_chunk(ChunkPtr& chunk, int rows_to_read, int64_t* rows_counted_out = nullptr);

    Status get_schema(std::vector<SlotDescriptor>* schema);

private:
    Status read_row(const avro::GenericRecord& record, const std::vector<AdaptiveNullableColumn*>& column_raw_ptrs);

    std::unique_ptr<avro::DataFileReader<avro::GenericDatum>> _file_reader = nullptr;
    bool _is_inited = false;

    // all belows are only used in read data
    std::string _filename = "";
    const std::vector<SlotDescriptor*>* _slot_descs = nullptr;
    const std::vector<avrocpp::ColumnReaderUniquePtr>* _column_readers = nullptr;
    size_t _num_of_columns_from_file = 0;
    bool _col_not_found_as_null = false;

    // reuse generic datum and field indexes for better performance
    std::unique_ptr<avro::GenericDatum> _datum = nullptr;
    std::vector<int64_t> _field_indexes;

    RuntimeState* _state = nullptr;
    ScannerCounter* _counter = nullptr;

    // Block-level record count precomputed in init() for the no-column (count(*)) path.
    // -1 means not precomputed; >= 0 means read_chunk() drains from here without file IO.
    int64_t _total_count = -1;
    int64_t _count_remaining = 0;

    // Split end position (exclusive byte offset).  0 means "read to end of file".
    // read_chunk() stops once _file_reader->pastSync(_split_end) is true.
    int64_t _split_end = 0;
};

using AvroReaderUniquePtr = std::unique_ptr<AvroReader>;

} // namespace starrocks
