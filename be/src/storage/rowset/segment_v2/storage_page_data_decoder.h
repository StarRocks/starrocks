// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/rowset/segment_v2/binary_dict_page.h"
#include "storage/rowset/segment_v2/bitshuffle_page.h"
#include "storage/rowset/segment_v2/page_handle.h"
#include "storage/rowset/segment_v2/page_pointer.h"
#include "util/slice.h"

namespace starrocks::segment_v2 {

class DataDecoder {
public:
    DataDecoder() = default;
    virtual ~DataDecoder() = default;

    virtual void reserve_head(uint8_t head_size) {}

    virtual Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                                    std::unique_ptr<char[]>* page, Slice* page_slice) {
        return Status::OK();
    }
};

class BitShuffleDataDecoder : public DataDecoder {
public:
    BitShuffleDataDecoder() = default;
    ~BitShuffleDataDecoder() = default;

    void reserve_head(uint8_t head_size) override {
        DCHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override {
        DataPageFooterPB data_footer = footer->data_page_footer();

        size_t num_elements = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 0);
        size_t compressed_size = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 4);
        size_t num_element_after_padding = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 8);
        size_t size_of_element = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 12);
        DCHECK_EQ(num_element_after_padding, ALIGN_UP(num_elements, 8U));

        size_t header_size = _reserve_head_size + BITSHUFFLE_PAGE_HEADER_SIZE;
        size_t data_size = num_element_after_padding * size_of_element;
        auto null_size = data_footer.nullmap_size();

        // data_size is size of decoded_data
        // compressed_size contains encoded_data size and BITSHUFFLE_PAGE_HEADER_SIZE
        std::unique_ptr<char[]> decompressed_page(
                new char[page_slice->size + data_size - (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE)]);
        memcpy(decompressed_page.get(), page_slice->data, header_size);

        Slice compressed_body(page_slice->data + header_size, compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE);
        Slice decompressed_body(&(decompressed_page.get()[header_size]), data_size);
        int64_t bytes = bitshuffle::decompress_lz4(compressed_body.data, decompressed_body.data,
                                                   num_element_after_padding, size_of_element, 0);
        if (bytes != compressed_body.size) {
            return Status::Corruption(
                    strings::Substitute("decompress failed: expected number of bytes consumed=$0 vs real consumed=$1",
                                        compressed_body.size, bytes));
        }

        memcpy(decompressed_body.data + decompressed_body.size,
               page_slice->data + header_size + (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE),
               null_size + footer_size);

        *page = std::move(decompressed_page);
        *page_slice = Slice(page->get(), header_size + data_size + null_size + footer_size);

        return Status::OK();
    }

private:
    uint8_t _reserve_head_size = 0;
};

class BinaryDictDataDecoder : public DataDecoder {
public:
    BinaryDictDataDecoder() {
        _bit_shuffle_decoder = std::make_unique<BitShuffleDataDecoder>();
        _bit_shuffle_decoder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
    }
    ~BinaryDictDataDecoder() = default;

    void reserve_head(uint8_t head_size) override {
        DCHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override {
        size_t type = decode_fixed32_le((const uint8_t*)&(page_slice->data[0]));
        if (type == DICT_ENCODING) {
            return _bit_shuffle_decoder->decode_page_data(footer, footer_size, encoding, page, page_slice);
        } else if (type == PLAIN_ENCODING) {
            return Status::OK();
        } else {
            LOG(WARNING) << "invalide encoding type:" << type;
            return Status::Corruption(strings::Substitute("invalid encoding type:$0", type));
        }
    }

private:
    uint8_t _reserve_head_size = 0;
    std::unique_ptr<BitShuffleDataDecoder> _bit_shuffle_decoder;
};

} // namespace starrocks::segment_v2