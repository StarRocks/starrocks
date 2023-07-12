// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "column/column.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/level_codec.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "util/compression/block_compression.h"

namespace starrocks {
class BlockCompressionCodec;
} // namespace starrocks

namespace starrocks::parquet {

class PageReader;
struct ColumnReaderOptions;

class ColumnChunkReader {
public:
    ColumnChunkReader(level_t max_def_level, level_t max_rep_level, int32_t type_length,
                      const tparquet::ColumnChunk* column_chunk, const ColumnReaderOptions& opts);
    ~ColumnChunkReader();

    Status init(int chunk_size);

    Status load_header();

    Status load_page();

    Status skip_page();

    bool current_page_is_dict();

    uint32_t num_values() const { return _num_values; }

    // Try to decode n definition levels into 'levels'
    // return number of decoded levels.
    // If the returned value is less than input n, this means current page don't have
    // enough levels.
    // User should call next_page() to get more levels
    size_t decode_def_levels(size_t n, level_t* levels) {
        DCHECK_GT(_max_def_level, 0);
        return _def_level_decoder.decode_batch(n, levels);
    }

    LevelDecoder& def_level_decoder() { return _def_level_decoder; }
    LevelDecoder& rep_level_decoder() { return _rep_level_decoder; }

    size_t decode_rep_levels(size_t n, level_t* levels) {
        DCHECK_GT(_max_rep_level, 0);
        return _rep_level_decoder.decode_batch(n, levels);
    }

    Status decode_values(size_t n, const uint8_t* is_nulls, ColumnContentType content_type, vectorized::Column* dst) {
        size_t idx = 0;
        while (idx < n) {
            bool is_null = is_nulls[idx++];
            size_t run = 1;
            while (idx < n && is_nulls[idx] == is_null) {
                idx++;
                run++;
            }
            if (is_null) {
                dst->append_nulls(run);
            } else {
                RETURN_IF_ERROR(_cur_decoder->next_batch(run, content_type, dst));
            }
        }
        return Status::OK();
    }

    Status decode_values(size_t n, ColumnContentType content_type, vectorized::Column* dst) {
        return _cur_decoder->next_batch(n, content_type, dst);
    }

    const tparquet::ColumnMetaData& metadata() const { return _chunk_metadata->meta_data; }

    Status get_dict_values(vectorized::Column* column) {
        RETURN_IF_ERROR(_try_load_dictionary());
        return _cur_decoder->get_dict_values(column);
    }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, const vectorized::NullableColumn& nulls,
                           vectorized::Column* column) {
        RETURN_IF_ERROR(_try_load_dictionary());
        return _cur_decoder->get_dict_values(dict_codes, nulls, column);
    }

    Status get_dict_codes(const std::vector<Slice>& dict_values, const vectorized::NullableColumn& nulls,
                          std::vector<int32_t>* dict_codes) {
        RETURN_IF_ERROR(_try_load_dictionary());
        return _cur_decoder->get_dict_codes(dict_values, nulls, dict_codes);
    }

private:
    Status _parse_page_header();
    Status _parse_page_data();

    Status _try_load_dictionary();
    Status _read_and_decompress_page_data();
    Status _parse_data_page();
    Status _parse_dict_page();

    Status _read_and_decompress_page_data(uint32_t compressed_size, uint32_t uncompressed_size, bool is_compressed);

private:
    enum PageParseState {
        INITIALIZED,
        PAGE_HEADER_PARSED,
        PAGE_LEVELS_PARSED,
        PAGE_DATA_PARSED,
    };

    level_t _max_def_level = 0;
    level_t _max_rep_level = 0;
    int32_t _type_length = 0;
    const tparquet::ColumnChunk* _chunk_metadata = nullptr;
    const ColumnReaderOptions& _opts;
    std::unique_ptr<PageReader> _page_reader;
    const BlockCompressionCodec* _compress_codec = nullptr;

    LevelDecoder _def_level_decoder;
    LevelDecoder _rep_level_decoder;

    int _chunk_size = 0;
    size_t _num_values = 0;

    std::vector<uint8_t> _compressed_buf;
    std::vector<uint8_t> _uncompressed_buf;

    PageParseState _page_parse_state = INITIALIZED;
    Slice _data;

    bool _dict_page_parsed = false;
    Decoder* _cur_decoder = nullptr;
    std::unordered_map<int, std::unique_ptr<Decoder>> _decoders;
};

} // namespace starrocks::parquet
