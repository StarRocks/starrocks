// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string_view>
#include <utility>
#include <vector>

#include "exec/vectorized/file_scanner.h"
#include "formats/csv/converter.h"
#include "util/logging.h"
#include "util/raw_container.h"

namespace starrocks {
class SequentialFile;
}

namespace starrocks::vectorized {

class CSVScanner final : public FileScanner {
#ifndef BE_TEST
    constexpr static size_t kMinBufferSize = 8 * 1024 * 1024L;
    constexpr static size_t kMaxBufferSize = 512 * 1024 * 1024L;
#else
    constexpr static size_t kMinBufferSize = 128 * 1024L;
    constexpr static size_t kMaxBufferSize = 512 * 1024L;
#endif

public:
    CSVScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
               ScannerCounter* counter);

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    void close() override{};

private:
    class Buffer {
    public:
        // Does NOT take the ownership of |buff|.
        Buffer(char* buff, size_t cap) : _begin(buff), _position(buff), _limit(buff), _end(buff + cap) {}

        void append(char c) { *_limit++ = c; }

        // Returns the number of bytes between the current position and the limit.
        size_t available() const { return _limit - _position; }

        // Returns the number of elements between the the limit and the end.
        size_t free_space() const { return _end - _limit; }

        // Returns this buffer's capacity.
        size_t capacity() const { return _end - _begin; }

        // Returns this buffer's read position.
        char* position() { return _position; }

        // Returns this buffer's write position.
        char* limit() { return _limit; }

        void add_limit(size_t n) { _limit += n; }

        // Finds the first character equal to the given character |c|. Search begins at |pos|.
        // Return: address of the first character of the found character or NULL if no such
        // character is found.
        char* find(char c, size_t pos = 0) { return (char*)memchr(position() + pos, c, available() - pos); }

        void skip(size_t n) { _position += n; }

        // Compacts this buffer.
        // The bytes between the buffer's current position and its limit, if any,
        // are copied to the beginning of the buffer.
        void compact() {
            size_t n = available();
            memmove(_begin, _position, available());
            _limit = _begin + n;
            _position = _begin;
        }

    private:
        char* _begin;
        char* _position; // next read position
        char* _limit;    // next write position
        char* _end;
    };

    class CSVReader {
    public:
        using Record = Slice;
        using Field = Slice;
        using Fields = std::vector<Field>;

        CSVReader(std::shared_ptr<SequentialFile> file, char record_delimiter, string field_delimiter)
                : _file(std::move(file)),
                  _record_delimiter(record_delimiter),
                  _field_delimiter(std::move(field_delimiter)),
                  _storage(kMinBufferSize),
                  _buff(_storage.data(), _storage.size()) {}

        Status next_record(Record* record);

        void set_limit(size_t limit) { _limit = limit; }

        void split_record(const Record& record, Fields* fields) const;

        void set_counter(ScannerCounter* counter) { _counter = counter; }

    private:
        Status _expand_buffer();
        Status _fill_buffer();

        std::shared_ptr<SequentialFile> _file;
        char _record_delimiter;
        string _field_delimiter;
        raw::RawVector<char> _storage;
        Buffer _buff;
        size_t _parsed_bytes = 0;
        size_t _limit = 0;
        ScannerCounter* _counter = nullptr;
    };

    ChunkPtr _create_chunk(const std::vector<SlotDescriptor*>& slots);

    Status _parse_csv(Chunk* chunk);
    ChunkPtr _materialize(ChunkPtr& src_chunk);
    void _report_error(const std::string& line, const std::string& err_msg);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    using CSVReaderPtr = std::unique_ptr<CSVReader>;

    const TBrokerScanRange& _scan_range;
    std::vector<Column*> _column_raw_ptrs;
    char _record_delimiter;
    string _field_delimiter;
    int _num_fields_in_csv = 0;
    int _curr_file_index = -1;
    CSVReaderPtr _curr_reader;
    std::vector<ConverterPtr> _converters;
};

} // namespace starrocks::vectorized
