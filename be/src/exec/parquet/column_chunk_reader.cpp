// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/parquet/column_chunk_reader.h"

#include <memory>

#include "env/env.h"
#include "exec/parquet/encoding.h"
#include "exec/parquet/encoding_dict.h"
#include "exec/parquet/page_reader.h"
#include "exec/parquet/types.h"
#include "exec/parquet/utils.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/runtime_profile.h"

namespace starrocks::parquet {

class CountedSeekableInputStream : public io::SeekableInputStreamWrapper {
public:
    explicit CountedSeekableInputStream(std::shared_ptr<io::SeekableInputStream> stream,
                                        vectorized::HdfsScanStats* stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership), _stream(stream), _stats(stats) {}

    ~CountedSeekableInputStream() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read(data, size));
        _stats->bytes_read += nread;
        return nread;
    }

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) override {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        _stats->io_count += 1;
        ASSIGN_OR_RETURN(auto nread, _stream->read_at(offset, data, size));
        _stats->bytes_read += nread;
        return nread;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    vectorized::HdfsScanStats* _stats;
};

ColumnChunkReader::ColumnChunkReader(level_t max_def_level, level_t max_rep_level, int32_t type_length,
                                     const tparquet::ColumnChunk* column_chunk, RandomAccessFile* file,
                                     const ColumnChunkReaderOptions& opts)
        : _max_def_level(max_def_level),
          _max_rep_level(max_rep_level),
          _type_length(type_length),
          _chunk_metadata(column_chunk),
          _opts(opts),
          _file(std::make_shared<CountedSeekableInputStream>(file->stream(), opts.stats), file->filename()) {}

ColumnChunkReader::~ColumnChunkReader() = default;

Status ColumnChunkReader::init(int chunk_size) {
    int64_t start_offset = 0;
    if (metadata().__isset.dictionary_page_offset) {
        start_offset = metadata().dictionary_page_offset;
    } else {
        start_offset = metadata().data_page_offset;
    }

    _page_reader = std::make_unique<PageReader>(&_file, start_offset, metadata().total_compressed_size);

    // seek to the first page
    _page_reader->seek_to_offset(start_offset);

    auto compress_type = convert_compression_codec(metadata().codec);
    RETURN_IF_ERROR(get_block_compression_codec(compress_type, &_compress_codec));

    RETURN_IF_ERROR(_try_load_dictionary(chunk_size));
    RETURN_IF_ERROR(_parse_page_data());
    return Status::OK();
}

Status ColumnChunkReader::next_page() {
    RETURN_IF_ERROR(_parse_page_header());
    RETURN_IF_ERROR(_parse_page_data());
    return Status::OK();
}

Status ColumnChunkReader::_parse_page_header() {
    DCHECK(_page_parse_state == INITIALIZED || _page_parse_state == PAGE_DATA_PARSED);
    RETURN_IF_ERROR(_page_reader->next_header());
    _page_parse_state = PAGE_HEADER_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_parse_page_data() {
    switch (_page_reader->current_header()->type) {
    case tparquet::PageType::DATA_PAGE:
        RETURN_IF_ERROR(_parse_data_page());
        break;
    case tparquet::PageType::DICTIONARY_PAGE:
        return Status::InternalError("There are two dictionary page in this column");
    default:
        return Status::NotSupported(
                strings::Substitute("Not supported page type: $0", _page_reader->current_header()->type));
        break;
    }
    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

void ColumnChunkReader::_reserve_uncompress_buf(size_t size) {
    if (size <= _uncompressed_buf_capacity) {
        return;
    }
    auto new_capacity = BitUtil::next_power_of_two(size);
    _uncompressed_buf.reset(new uint8_t[new_capacity]);
    _uncompressed_buf_capacity = new_capacity;
}

Status ColumnChunkReader::_read_and_decompress_page_data(uint32_t compressed_size, uint32_t uncompressed_size,
                                                         bool is_compressed) {
    RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read and decompress page"));
    if (is_compressed && _compress_codec != nullptr) {
        Slice com_slice("", compressed_size);
        RETURN_IF_ERROR(_page_reader->read_bytes((const uint8_t**)&com_slice.data, com_slice.size));

        _reserve_uncompress_buf(uncompressed_size);
        _data = Slice(_uncompressed_buf.get(), uncompressed_size);
        RETURN_IF_ERROR(_compress_codec->decompress(com_slice, &_data));
    } else {
        _data.size = uncompressed_size;
        _page_reader->read_bytes((const uint8_t**)&_data.data, _data.size);
    }

    return Status::OK();
}

Status ColumnChunkReader::_parse_data_page() {
    if (_page_parse_state == PAGE_DATA_PARSED) {
        return Status::OK();
    }
    if (_page_parse_state != PAGE_HEADER_PARSED) {
        return Status::InternalError("Error state");
    }

    const auto& header = *_page_reader->current_header();

    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    RETURN_IF_ERROR(_read_and_decompress_page_data(compressed_size, uncompressed_size, true));

    // parse levels
    if (_max_rep_level > 0) {
        RETURN_IF_ERROR(_rep_level_decoder.parse(header.data_page_header.repetition_level_encoding, _max_rep_level,
                                                 header.data_page_header.num_values, &_data));
    }
    if (_max_def_level > 0) {
        RETURN_IF_ERROR(_def_level_decoder.parse(header.data_page_header.definition_level_encoding, _max_def_level,
                                                 header.data_page_header.num_values, &_data));
    }

    _num_values = header.data_page_header.num_values;
    auto encoding = header.data_page_header.encoding;
    // change the deprecated encoding to RLE_DICTIONARY
    if (encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        encoding = tparquet::Encoding::RLE_DICTIONARY;
    }

    _cur_decoder = _decoders[static_cast<int>(encoding)].get();
    if (_cur_decoder == nullptr) {
        std::unique_ptr<Decoder> decoder;
        const EncodingInfo* enc_info = nullptr;
        RETURN_IF_ERROR(EncodingInfo::get(metadata().type, encoding, &enc_info));
        RETURN_IF_ERROR(enc_info->create_decoder(&decoder));

        _cur_decoder = decoder.get();
        _decoders[static_cast<int>(encoding)] = std::move(decoder);
    }

    _cur_decoder->set_type_legth(_type_length);
    _cur_decoder->set_data(_data);

    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_parse_dict_page() {
    const tparquet::PageHeader& header = *_page_reader->current_header();
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header.type);

    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    RETURN_IF_ERROR(_read_and_decompress_page_data(compressed_size, uncompressed_size, true));
    _page_parse_state = PAGE_DATA_PARSED;
    return Status::OK();
}

Status ColumnChunkReader::_try_load_dictionary(int chunk_size) {
    RETURN_IF_ERROR(_parse_page_header());
    const auto& header = *_page_reader->current_header();
    if (header.type != tparquet::PageType::DICTIONARY_PAGE) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_parse_dict_page());

    // initialize dict decoder to decode dictionary
    std::unique_ptr<Decoder> dict_decoder;

    tparquet::Encoding::type dict_encoding = header.dictionary_page_header.encoding;
    // Using the PLAIN_DICTIONARY enum value is deprecated in the Parquet 2.0 specification.
    // Prefer using RLE_DICTIONARY in a data page and PLAIN in a dictionary page for Parquet 2.0+ files.
    // refer: https://github.com/apache/parquet-format/blob/master/Encodings.md
    if (dict_encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        dict_encoding = tparquet::Encoding::PLAIN;
    }

    const EncodingInfo* code_info = nullptr;
    RETURN_IF_ERROR(EncodingInfo::get(metadata().type, dict_encoding, &code_info));
    RETURN_IF_ERROR(code_info->create_decoder(&dict_decoder));
    dict_decoder->set_data(_data);
    dict_decoder->set_type_legth(_type_length);

    // initialize decoder
    std::unique_ptr<Decoder> decoder;
    RETURN_IF_ERROR(EncodingInfo::get(metadata().type, tparquet::Encoding::RLE_DICTIONARY, &code_info));
    RETURN_IF_ERROR(code_info->create_decoder(&decoder));
    RETURN_IF_ERROR(decoder->set_dict(chunk_size, header.dictionary_page_header.num_values, dict_decoder.get()));
    _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)] = std::move(decoder);

    RETURN_IF_ERROR(_parse_page_header());
    return Status::OK();
}

} // namespace starrocks::parquet
