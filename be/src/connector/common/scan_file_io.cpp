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

#include "connector/common/scan_file_io.h"

#include <memory>

#include "base/compression/stream_decompressor.h"
#include "cache/scan/cache_select_input_stream.hpp"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/config_cache_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/runtime_profile.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "io/compressed_input_stream.h"

namespace starrocks::connector {

namespace {

class CountedSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit CountedSeekableInputStream(const std::shared_ptr<io::SeekableInputStream>& stream,
                                        FormatScannerStats* stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(stream), _stats(stats) {}

    ~CountedSeekableInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read(data, size));
        _stats->bytes_read += nread;
        return nread;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        _stats->bytes_read += size;
        return _stream->read_at_fully(offset, data, size);
    }

    StatusOr<std::string_view> peek(int64_t count) override { return _stream->peek(count); }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read_at(offset, out, count));
        _stats->bytes_read += nread;
        return nread;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    FormatScannerStats* _stats;
};

} // namespace

StatusOr<std::unique_ptr<RandomAccessFile>> open_scan_random_access_file(
        std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<CacheInputStream>& cache_input_stream, const ScanFileOpenOptions& options) {
    ASSIGN_OR_RETURN(std::unique_ptr<RandomAccessFile> raw_file, options.fs->new_random_access_file(options.file_path));
    int64_t file_size = options.file_size;
    if (file_size < 0) {
        ASSIGN_OR_RETURN(file_size, raw_file->stream()->get_size());
    }
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();
    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, options.fs_stats);

    shared_buffered_input_stream = std::make_shared<SharedBufferedInputStream>(input_stream, filename, file_size);
    const SharedBufferedInputStream::CoalesceOptions shared_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(shared_options);
    input_stream = shared_buffered_input_stream;

    const DataCacheOptions& datacache_options = options.datacache_options;
    if (datacache_options.enable_datacache) {
        if (datacache_options.enable_cache_select) {
            cache_input_stream = std::make_shared<CacheSelectInputStream>(
                    shared_buffered_input_stream, filename, file_size, datacache_options.modification_time);
        } else {
            cache_input_stream = std::make_shared<CacheInputStream>(shared_buffered_input_stream, filename, file_size,
                                                                    datacache_options.modification_time);
            cache_input_stream->set_enable_populate_cache(datacache_options.enable_populate_datacache);
            cache_input_stream->set_enable_async_populate_mode(datacache_options.enable_datacache_async_populate_mode);
            cache_input_stream->set_enable_cache_io_adaptor(datacache_options.enable_datacache_io_adaptor);
            cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
            input_stream = cache_input_stream;
        }
        cache_input_stream->set_priority(datacache_options.datacache_priority);
        cache_input_stream->set_ttl_seconds(datacache_options.datacache_ttl_seconds);
        shared_buffered_input_stream->set_align_size(cache_input_stream->get_align_size());
    }

    if (options.compression_type != CompressionTypePB::NO_COMPRESSION) {
        using DecompressorPtr = std::shared_ptr<StreamDecompressor>;
        ASSIGN_OR_RETURN(auto dec, StreamDecompressor::create_decompressor(options.compression_type));
        auto compressed_input_stream =
                std::make_shared<io::CompressedInputStream>(input_stream, DecompressorPtr(dec.release()));
        input_stream = std::make_shared<io::CompressedSeekableInputStream>(compressed_input_stream);
    }

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, options.app_stats);
    auto file = std::make_unique<RandomAccessFile>(input_stream, filename);
    file->set_size(file_size);
    return file;
}

} // namespace starrocks::connector
