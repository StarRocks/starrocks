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

#include "exec/cache_select_scanner.h"

#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "formats/parquet/file_reader.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks {

Status CacheSelectScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

Status CacheSelectScanner::do_open(RuntimeState* runtime_state) {
    bool cache_select_supported_format = _scanner_params.scan_range->file_format == THdfsFileFormat::PARQUET ||
                                         _scanner_params.scan_range->file_format == THdfsFileFormat::ORC ||
                                         _scanner_params.scan_range->file_format == THdfsFileFormat::TEXT;
    if (!cache_select_supported_format) {
        // note: return OK instead of an error to bypass the cache select.
        // this logic cannot be moved to the upper-level call.
        return Status::OK();
    }

    // Cache select don't need to decompress data
    _compression_type = CompressionTypePB::NO_COMPRESSION;
    RETURN_IF_ERROR(open_random_access_file());
    return Status::OK();
}

void CacheSelectScanner::do_update_counter(HdfsScanProfile* profile) {
    const std::string prefix = "CacheSelect";
    RuntimeProfile* root_profile = profile->runtime_profile;
    ADD_COUNTER(root_profile, prefix, TUnit::NONE);

    do_update_iceberg_v2_counter(root_profile, prefix);
}

void CacheSelectScanner::do_close(RuntimeState* runtime_state) noexcept {}

Status CacheSelectScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_scanner_params.scan_range->file_format == THdfsFileFormat::TEXT) {
        RETURN_IF_ERROR(_fetch_textfile());
    } else if (_scanner_params.scan_range->file_format == THdfsFileFormat::PARQUET) {
        RETURN_IF_ERROR(_fetch_parquet());
    } else if (_scanner_params.scan_range->file_format == THdfsFileFormat::ORC) {
        RETURN_IF_ERROR(_fetch_orc());
    } else {
        // note: return EOF instead of an error to bypass the cache select.
        // this logic cannot be moved to the upper-level call.
        return Status::EndOfFile("Unsupported file format in cache select: " +
                                 to_string(_scanner_params.scan_range->file_format));
    }

    // handle iceberg delete files
    if (!_scanner_params.deletes.empty()) {
        RETURN_IF_ERROR(_fetch_iceberg_delete_files());
    }

    return Status::EndOfFile("");
}

Status CacheSelectScanner::_fetch_orc() {
    std::unique_ptr<ORCHdfsFileStream> input_stream = std::make_unique<ORCHdfsFileStream>(
            _file.get(), _file->get_size().value(), _shared_buffered_input_stream.get());
    input_stream->set_app_stats(&_app_stats);

    std::unique_ptr<orc::Reader> reader;
    try {
        orc::ReaderOptions options;
        reader = orc::createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        return Status::InternalError(
                strings::Substitute("CacheSelectScanner::_fetch_orc failed. reason = $0", e.what()));
    }

    // prepare SlotDescriptor
    std::vector<SlotDescriptor*> slot_descriptors{};

    // resolve columns
    {
        std::unordered_set<std::string> known_column_names;
        OrcChunkReader::build_column_name_set(&known_column_names, _scanner_ctx.hive_column_names, reader->getType(),
                                              _scanner_ctx.case_sensitive, _scanner_ctx.orc_use_column_names);
        RETURN_IF_ERROR(_scanner_ctx.update_materialized_columns(known_column_names));
        ASSIGN_OR_RETURN(auto skip, _scanner_ctx.should_skip_by_evaluating_not_existed_slots());
        if (skip) {
            LOG(INFO) << "CacheSelectScanner: orc skip file for non existed slot conjuncts.";
            return Status::EndOfFile("");
        }

        for (const auto& column : _scanner_ctx.materialized_columns) {
            const auto col_name = Utils::format_name(column.name(), _scanner_ctx.case_sensitive);
            if (known_column_names.contains(col_name)) {
                slot_descriptors.emplace_back(column.slot_desc);
            }
        }
    }

    // get selected column ids
    std::set<uint64_t> selected_column_ids;
    {
        std::list<uint64_t> selected_leaf_column_ids{};
        OrcMappingOptions orc_mapping_options{};
        orc_mapping_options.case_sensitive = _scanner_ctx.case_sensitive;
        orc_mapping_options.filename = _file->filename();
        orc_mapping_options.invalid_as_null = true;
        ASSIGN_OR_RETURN(
                std::unique_ptr<OrcMapping> orc_mapping,
                OrcMappingFactory::build_mapping(slot_descriptors, reader->getType(), _scanner_ctx.orc_use_column_names,
                                                 _scanner_ctx.hive_column_names, orc_mapping_options));

        for (size_t i = 0; i < slot_descriptors.size(); i++) {
            SlotDescriptor* desc = slot_descriptors[i];
            // column not existed in orc file, ignore this SlotDescriptor
            if (!orc_mapping->contains(i)) continue;
            RETURN_IF_ERROR(orc_mapping->set_include_column_id(i, desc->type(), &selected_leaf_column_ids));
        }

        orc::RowReaderOptions row_reader_options{};
        row_reader_options.includeTypes(selected_leaf_column_ids);
        std::unique_ptr<orc::RowReader> row_reader = reader->createRowReader(row_reader_options);
        // get selected column ids from RowReader
        for (uint64_t i = 0; i < row_reader->getSelectedColumns().size(); i++) {
            if (row_reader->getSelectedColumns()[i]) {
                selected_column_ids.emplace(i);
            }
        }
    }

    std::vector<DiskRange> disk_ranges{};
    {
        uint64_t stripe_number = reader->getNumberOfStripes();
        std::vector<DiskRange> stripe_disk_ranges{};

        const auto* scan_range = _scanner_ctx.scan_range;
        size_t scan_start = scan_range->offset;
        size_t scan_end = scan_start + scan_range->length;

        for (uint64_t idx = 0; idx < stripe_number; idx++) {
            const auto& stripe_info = reader->getStripe(idx);
            uint64_t stripe_offset = stripe_info->getOffset();
            // Read stripes in this scan range
            if (stripe_offset >= scan_start && stripe_offset < scan_end) {
                int64_t length = stripe_info->getLength();
                _app_stats.orc_stripe_sizes.push_back(length);

                uint64_t cur_offset = stripe_offset;
                for (uint64_t stream_id = 0; stream_id < stripe_info->getNumberOfStreams(); stream_id++) {
                    const auto& stream_info = stripe_info->getStreamInformation(stream_id);
                    // put selected column's stream into disk_ranges
                    if (selected_column_ids.contains(stream_info->getColumnId())) {
                        disk_ranges.emplace_back(cur_offset, stream_info->getLength());
                    }
                    cur_offset += stream_info->getLength();
                }
            }
        }
    }

    return _write_disk_ranges(_shared_buffered_input_stream, _cache_input_stream, disk_ranges);
}

Status CacheSelectScanner::_fetch_parquet() {
    // create file reader
    std::shared_ptr<parquet::FileReader> reader = std::make_shared<parquet::FileReader>(
            4096, _file.get(), _file->get_size().value(), _scanner_params.datacache_options,
            _shared_buffered_input_stream.get(), nullptr);

    RETURN_IF_ERROR(reader->init(&_scanner_ctx));

    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    RETURN_IF_ERROR(reader->collect_scan_io_ranges(&io_ranges));

    std::vector<DiskRange> disk_ranges{};
    for (const auto& io_range : io_ranges) {
        disk_ranges.emplace_back(io_range.offset, io_range.size);
    }

    return _write_disk_ranges(_shared_buffered_input_stream, _cache_input_stream, disk_ranges);
}

// Split text into multiply disk ranges, then fetch it
Status CacheSelectScanner::_fetch_textfile() {
    // If it's compressed file, we only handle scan range whose offset == 0.
    if (get_compression_type_from_path(_scanner_params.path) != UNKNOWN_COMPRESSION &&
        _scanner_params.scan_range->offset != 0) {
        return Status::OK();
    }

    const int64_t start_offset = _scanner_params.scan_range->offset;
    const int64_t end_offset = _scanner_params.scan_range->offset + _scanner_params.scan_range->length;

    std::vector<DiskRange> disk_ranges{};
    for (int64_t offset = start_offset; offset < end_offset;) {
        const int64_t remain_length =
                std::min(static_cast<int64_t>(config::io_coalesce_read_max_buffer_size), end_offset - offset);
        disk_ranges.emplace_back(offset, remain_length);
        offset += remain_length;
    }

    return _write_disk_ranges(_shared_buffered_input_stream, _cache_input_stream, disk_ranges);
}

// for iceberg delete files, we fetch an entire file directly
Status CacheSelectScanner::_fetch_iceberg_delete_files() {
    for (const auto* delete_file : _scanner_params.deletes) {
        RETURN_IF_ERROR(_write_entire_file(delete_file->full_path, delete_file->length));
    }

    _app_stats.iceberg_delete_files_per_scan += _scanner_params.deletes.size();
    return Status::OK();
}

Status CacheSelectScanner::_write_entire_file(const std::string& file_path, size_t file_size) {
    OpenFileOptions options{};
    options.fs = _scanner_params.fs;
    options.path = file_path;
    options.file_size = file_size;
    options.datacache_options = _scanner_params.datacache_options;
    options.fs_stats = &_fs_stats;
    options.app_stats = &_app_stats;

    std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<io::CacheInputStream> cache_input_stream;
    ASSIGN_OR_RETURN(auto dummy_file,
                     create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));

    std::vector<DiskRange> disk_ranges{};
    disk_ranges.emplace_back(0, file_size);

    return _write_disk_ranges(shared_buffered_input_stream, cache_input_stream, disk_ranges);
}

Status CacheSelectScanner::_write_disk_ranges(std::shared_ptr<io::SharedBufferedInputStream>& shared_input_stream,
                                              std::shared_ptr<io::CacheInputStream>& cache_input_stream,
                                              const std::vector<DiskRange>& disk_ranges) {
    std::vector<DiskRange> merged_disk_ranges{};
    DiskRangeHelper::merge_adjacent_disk_ranges(disk_ranges, config::io_coalesce_read_max_distance_size,
                                                config::io_coalesce_read_max_buffer_size, merged_disk_ranges);

    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    for (const DiskRange& disk_range : merged_disk_ranges) {
        io_ranges.emplace_back(disk_range.offset(), disk_range.length());
    }
    RETURN_IF_ERROR(shared_input_stream->set_io_ranges(io_ranges));

    io::CacheSelectInputStream* cache_select_input_stream =
            down_cast<io::CacheSelectInputStream*>(cache_input_stream.get());

    for (const DiskRange& merged_disk_range : merged_disk_ranges) {
        RETURN_IF_ERROR(
                cache_select_input_stream->write_at_fully(merged_disk_range.offset(), merged_disk_range.length()));
    }
    return Status::OK();
}

} // namespace starrocks
