// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "gen_cpp/segment_v2.pb.h"
#include "storage/page_cache.h"
#include "storage/rowset/segment_v2/binary_dict_page.h"
#include "storage/rowset/segment_v2/bitshuffle_page.h"
#include "storage/rowset/segment_v2/page_handle.h"
#include "storage/rowset/segment_v2/page_pointer.h"
#include "util/slice.h"

namespace starrocks {

namespace segment_v2 {

class DataDecoder {
public:
    DataDecoder() = default;
    virtual ~DataDecoder() = default;

    virtual void reserve_head(uint8_t head_size) { CHECK(false) << "reserve_head() not supported"; }

    virtual Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                                    std::unique_ptr<char[]>* page, Slice& page_slice) = 0;

    static Status decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                              std::unique_ptr<char[]>* page, Slice& page_slice);
};

std::unique_ptr<DataDecoder> get_data_decoder(EncodingTypePB encoding);

class BinaryPlainDecoder : public DataDecoder {
public:
    BinaryPlainDecoder() = default;
    ~BinaryPlainDecoder() = default;

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        return Status::OK();
    }
};

class BinaryPrefixDecoder : public DataDecoder {
public:
    BinaryPrefixDecoder() = default;
    ~BinaryPrefixDecoder() = default;

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        return Status::OK();
    }
};

class FrameOfReferenceDecoder : public DataDecoder {
public:
    FrameOfReferenceDecoder() = default;
    ~FrameOfReferenceDecoder() = default;

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        return Status::OK();
    }
};

class RleDecoder : public DataDecoder {
public:
    RleDecoder() = default;
    ~RleDecoder() = default;

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        return Status::OK();
    }
};

class DefaultEncodingDecoder : public DataDecoder {
public:
    DefaultEncodingDecoder() = default;
    ~DefaultEncodingDecoder() = default;

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        return Status::OK();
    }
};

class BitShuffleDecoder : public DataDecoder {
public:
    BitShuffleDecoder() = default;
    ~BitShuffleDecoder() = default;

    void reserve_head(uint8_t head_size) override {
        CHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        DataPageFooterPB data_footer = footer->data_page_footer();

        size_t num_elements = decode_fixed32_le((const uint8_t*)&page_slice[_reserve_head_size + 0]);
        size_t compressed_size = decode_fixed32_le((const uint8_t*)&page_slice[_reserve_head_size + 4]);
        size_t num_element_after_padding = decode_fixed32_le((const uint8_t*)&page_slice[_reserve_head_size + 8]);
        size_t size_of_element = decode_fixed32_le((const uint8_t*)&page_slice[_reserve_head_size + 12]);
        DCHECK_EQ(num_element_after_padding, ALIGN_UP(num_elements, 8U));

        size_t header_size = _reserve_head_size + BITSHUFFLE_PAGE_HEADER_SIZE;
        size_t data_size = num_element_after_padding * size_of_element;
        auto null_size = data_footer.nullmap_size();

        std::unique_ptr<char[]> decompressed_page(
                new char[page_slice.size + data_size - (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE)]);
        memcpy(decompressed_page.get(), page_slice.data, header_size);

        Slice compressed_body(page_slice.data + header_size, compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE);
        Slice decompressed_body(&(decompressed_page.get()[header_size]), data_size);
        int64_t bytes = bitshuffle::decompress_lz4(compressed_body.data, decompressed_body.data,
                                                   num_element_after_padding, size_of_element, 0);
        if (bytes != compressed_body.size) {
            return Status::Corruption(
                    strings::Substitute("decompress failed: expected number of bytes consumed=&0 vs real consumed=$1",
                                        compressed_body.size, bytes));
        }

        memcpy(decompressed_body.data + decompressed_body.size,
               page_slice.data + header_size + (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE),
               null_size + footer_size);
        //delete[] page_slice.data;
        *page = std::move(decompressed_page);
        page_slice = Slice(page->get(), header_size + data_size + null_size + footer_size);

        return Status::OK();
    }

private:
    uint8_t _reserve_head_size = 0;
};

class BinaryDictDecoder : public DataDecoder {
public:
    BinaryDictDecoder() = default;
    ~BinaryDictDecoder() = default;

    void reserve_head(uint8_t head_size) override {
        CHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }

    Status decode_data_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice& page_slice) override {
        size_t type = decode_fixed32_le((const uint8_t*)&page_slice.data[0]);
        if (type == DICT_ENCODING) {
            _data_decoder = std::make_unique<BitShuffleDecoder>();
            _data_decoder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
        } else if (type == PLAIN_ENCODING) {
            _data_decoder = std::make_unique<BinaryPlainDecoder>();
        } else {
            LOG(WARNING) << "invalide encoding type:" << type;
            return Status::Corruption(strings::Substitute("invalid encoding type:$0", type));
        }
        return _data_decoder->decode_data_page(footer, footer_size, encoding, page, page_slice);
    }

private:
    uint8_t _reserve_head_size = 0;
    std::unique_ptr<DataDecoder> _data_decoder;
};

} // namespace segment_v2
} // namespace starrocks