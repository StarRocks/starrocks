// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/parquet/column_chunk_reader.h"

#include <memory>

#include "column/column.h"
#include "env/env.h"
#include "exec/parquet/encoding.h"
#include "exec/parquet/encoding_dict.h"
#include "exec/parquet/page_reader.h"
#include "exec/parquet/types.h"
#include "exec/parquet/utils.h"
#include "gutil/strings/substitute.h"
#include "util/runtime_profile.h"

namespace starrocks::parquet {

class RandomAccessFileWrapper : public RandomAccessFile {
public:
    RandomAccessFileWrapper(RandomAccessFile* file, vectorized::HdfsScanStats* stats) : _file(file), _stats(stats) {}

    ~RandomAccessFileWrapper() override = default;

    Status read(uint64_t offset, Slice* res) const override {
        Status st;
        {
            SCOPED_RAW_TIMER(&_stats->io_ns);
            _stats->io_count += 1;
            st = _file->read(offset, res);
            _stats->bytes_read_from_disk += res->size;
        }
        return st;
    }

    Status read_at(uint64_t offset, const Slice& result) const override {
        Status st;
        {
            SCOPED_RAW_TIMER(&_stats->io_ns);
            _stats->io_count += 1;
            st = _file->read_at(offset, result);
            _stats->bytes_read_from_disk += result.size;
        }
        return st;
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        Status st;
        {
            SCOPED_RAW_TIMER(&_stats->io_ns);
            _stats->io_count += 1;
            st = _file->readv_at(offset, res, res_cnt);
            for (int i = 0; i < res_cnt; ++i) {
                _stats->bytes_read_from_disk += res[i].size;
            }
        }
        return st;
    }

    // Return the size of this file
    Status size(uint64_t* size) const override { return _file->size(size); }

    // Return name of this file
    const std::string& file_name() const override { return _file->file_name(); }

private:
    RandomAccessFile* _file;
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
          _file(new RandomAccessFileWrapper(file, opts.stats)) {}

ColumnChunkReader::~ColumnChunkReader() = default;

Status ColumnChunkReader::init() {
    int64_t start_offset = 0;
    if (metadata().__isset.dictionary_page_offset) {
        start_offset = metadata().dictionary_page_offset;
    } else {
        start_offset = metadata().data_page_offset;
    }

    _page_reader = std::make_unique<PageReader>(_file.get(), start_offset, metadata().total_compressed_size);

    // seek to the first page
    _page_reader->seek_to_offset(start_offset);

    auto compress_type = convert_compression_codec(metadata().codec);
    RETURN_IF_ERROR(get_block_compression_codec(compress_type, &_compress_codec));

    RETURN_IF_ERROR(_try_load_dictionary());
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
                strings::Substitute("Not supproted page type: $0", _page_reader->current_header()->type));
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

Status ColumnChunkReader::_try_load_dictionary() {
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
    RETURN_IF_ERROR(decoder->set_dict(header.dictionary_page_header.num_values, dict_decoder.get()));
    _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)] = std::move(decoder);

    RETURN_IF_ERROR(_parse_page_header());
    return Status::OK();
}

} // namespace starrocks::parquet
