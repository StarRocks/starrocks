#include <iostream>
#include <memory>

#include "exec/file_scanner.h"
#include "formats/csv/csv_reader.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {

#define CSVBENCHMARK

static size_t buffer_size = 15 * 1024 * 1024 * 1024UL;
class BenchCSVReader : public CSVReader {
public:
    BenchCSVReader(std::shared_ptr<SequentialFile> file, const std::string& record_delimiter,
                   const std::string& field_delimiter, bool trim_space, char escape, char enclose,
                   const size_t bufferSize)
            : CSVReader(CSVParseOptions(record_delimiter, field_delimiter, 0, trim_space, escape, enclose),
                        bufferSize) {
        _file = std::move(file);
    }

    Status _fill_buffer() override;

private:
    std::shared_ptr<SequentialFile> _file;
};

Status BenchCSVReader::_fill_buffer() {
    DCHECK(_buff.free_space() > 0);
    Slice s(_buff.limit(), _buff.free_space());
    auto res = _file->read(s.data, s.size);
    // According to the specification of `FileSystem::read`, when reached the end of
    // a file, the returned status will be OK instead of EOF, but here we check
    // EOF also for safety.
    if (res.status().is_end_of_file()) {
        s.size = 0;
    } else if (!res.ok()) {
        return res.status();
    } else {
        s.size = *res;
    }
    _buff.add_limit(s.size);
    auto n = _buff.available();
    if (s.size == 0) {
        if (n == 0) {
            // Has reached the end of file and the buffer is empty.
            return Status::EndOfFile(_file->filename());
        } else if (n < _row_delimiter_length ||
                   _buff.find(_parse_options.row_delimiter, n - _row_delimiter_length) == nullptr) {
            // Has reached the end of file but still no record delimiter found, which
            // is valid, according the RFC, add the record delimiter ourself.
            for (char ch : _parse_options.row_delimiter) {
                _buff.append(ch);
            }
        }
    }
    return Status::OK();
}

class BenchScanner final : public FileScanner {
public:
    BenchScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                 ScannerCounter* counter, size_t bufferSize)
            : FileScanner(state, profile, scan_range.params, counter),
              _scan_range(scan_range),
              _bufferSize(bufferSize) {
        if (scan_range.params.__isset.multi_column_separator) {
            _field_delimiter = scan_range.params.multi_column_separator;
        } else {
            _field_delimiter = scan_range.params.column_separator;
        }
        if (scan_range.params.__isset.multi_row_delimiter) {
            _record_delimiter = scan_range.params.multi_row_delimiter;
        } else {
            _record_delimiter = scan_range.params.row_delimiter;
        }
    }

    Status open() {
        std::shared_ptr<SequentialFile> file;
        TNetworkAddress address;
        TBrokerScanRangeParams params;
        const TBrokerRangeDesc& range_desc = _scan_range.ranges[0];
        Status st = create_sequential_file(range_desc, address, params, &file);
        if (!st.ok()) {
            std::cout << "Failed to create sequential files. status: " << st.to_string() << std::endl;
            ;
            return st;
        }
        _csv_reader =
                std::make_unique<BenchCSVReader>(file, _record_delimiter, _field_delimiter, false, 0, 0, _bufferSize);
        return st;
    }

    // Do nothing
    StatusOr<ChunkPtr> get_next() { return Status::OK(); };

    Status init_buffer() { return _csv_reader->init_buff(); }

    Status get_all_v1(int64_t& read_row_cnt) {
        CSVReader::Record record;
        Status st = Status::OK();
        CSVReader::Fields fields;
        while (true) {
            st = _csv_reader->next_record(&record);
            if (!st.ok()) {
                break;
            }
            fields.clear();
            _csv_reader->split_record(record, &fields);
            read_row_cnt++;
        }
        return st;
    }

    Status get_all_v2(int64_t& read_row_cnt) {
        CSVRow row;
        Status st = Status::OK();
        while (true) {
            st = _csv_reader->next_record(row);
            if (!st.ok()) {
                break;
            }
            read_row_cnt++;
        }
        return st;
    }

    void close() { FileScanner::close(); }

private:
    TBrokerScanRange _scan_range;
    std::string _record_delimiter;
    std::string _field_delimiter;
    std::unique_ptr<BenchCSVReader> _csv_reader;
    size_t _bufferSize;
};

std::unique_ptr<BenchScanner> create_bench_scanner(const std::vector<TBrokerRangeDesc>& ranges, const size_t buffSize,
                                                   const std::string& multi_row_delimiter = "\n",
                                                   const std::string& multi_column_separator = "|") {
    // Init RuntimeState
    ObjectPool _obj_pool;
    RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
    state->init_instance_mem_tracker();

    /// TBrokerScanRangeParams
    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
    params->__set_multi_row_delimiter(multi_row_delimiter);
    params->__set_multi_column_separator(multi_column_separator);
    params->strict_mode = true;
    RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("csv_bench_prof", true));
    ScannerCounter* counter = _obj_pool.add(new ScannerCounter());
    TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = ranges;
    return std::make_unique<BenchScanner>(state, profile, *broker_scan_range, counter, buffSize);
}

static std::unique_ptr<BenchScanner> init_bench_scanner(std::string filename) {
    std::string record_delimiter = ",";
    std::string field_delimiter = "\n";

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path(filename);
    range_one.__set_start_offset(0);
    range_one.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    range_one.__set_file_type(TFileType::FILE_LOCAL);
    ranges.push_back(range_one);

    return create_bench_scanner(ranges, buffer_size);
}

} // namespace starrocks

using namespace starrocks;

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " [file]"
                  << " [parser_version]" << std::endl;
        exit(1);
    }
    std::string filename = argv[1];
    std::unique_ptr<BenchScanner> scanner = init_bench_scanner(filename);
    Status st = scanner->open();
    if (!st.ok()) {
        std::cout << "Open scanner error. status: " << st.to_string();
    }
    // Benchmark 1: File IO
    auto start = std::chrono::system_clock::now();
    st = scanner->init_buffer();
    if (!st.ok() && !st.is_end_of_file()) {
        std::cout << "Init buffer error. status: " << st.to_string();
        return -1;
    } else {
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = end - start;
        std::cout << "FIle IO: " << diff.count() << std::endl;
    }
    // Benchmark 2: Parsing
    std::string version = argv[2];
    int64_t read_row_cnt = 0;
    start = std::chrono::system_clock::now();
    if (version == "v1") {
        st = scanner->get_all_v1(read_row_cnt);
    } else {
        st = scanner->get_all_v2(read_row_cnt);
    }
    if (!st.ok() && !st.is_end_of_file()) {
        std::cout << "Scanner get all error. status: " << st.to_string();
    } else {
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = end - start;
        std::cout << "Have read " << read_row_cnt << " records" << std::endl;
        std::cout << "Parsing: " << diff.count() << std::endl;
    }
} // namespace starrocks
