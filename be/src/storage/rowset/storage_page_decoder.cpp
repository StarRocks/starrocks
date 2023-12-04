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

#include "storage/rowset/storage_page_decoder.h"

#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/rowset/bitshuffle_wrapper.h"
#include "util/coding.h"

namespace starrocks {

class BitShuffleDataDecoder : public DataDecoder {
public:
    BitShuffleDataDecoder(PageTypePB page_type) : _page_type(page_type) {}
    ~BitShuffleDataDecoder() override = default;

    size_t get_null_size(PageFooterPB* footer) {
        if (_page_type == DICTIONARY_PAGE) {
            return 0;
        } else {
            const DataPageFooterPB& data_footer = footer->data_page_footer();
            return data_footer.nullmap_size();
        }
    }

    void reserve_head(uint8_t head_size) override {
        DCHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }
    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override {
        size_t num_elements = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 0);
        size_t compressed_size = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 4);
        size_t num_element_after_padding = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 8);
        size_t size_of_element = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 12);
        DCHECK_EQ(num_element_after_padding, ALIGN_UP(num_elements, 8U));

        size_t header_size = _reserve_head_size + BITSHUFFLE_PAGE_HEADER_SIZE;
        size_t data_size = num_element_after_padding * size_of_element;

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

        auto null_size = get_null_size(footer);
        memcpy(decompressed_body.data + decompressed_body.size,
               page_slice->data + header_size + (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE),
               null_size + footer_size);

        *page = std::move(decompressed_page);
        *page_slice = Slice(page->get(), header_size + data_size + null_size + footer_size);

        return Status::OK();
    }

private:
    PageTypePB _page_type;
    uint8_t _reserve_head_size = 0;
};

class DictDictDecoder : public DataDecoder {
public:
    DictDictDecoder() { _bit_shuffle_decoder = std::make_unique<BitShuffleDataDecoder>(DICTIONARY_PAGE); }
    ~DictDictDecoder() override = default;

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override {
        return _bit_shuffle_decoder->decode_page_data(footer, footer_size, encoding, page, page_slice);
    }

private:
    std::unique_ptr<BitShuffleDataDecoder> _bit_shuffle_decoder;
};

class BinaryDictDataDecoder : public DataDecoder {
public:
    BinaryDictDataDecoder() {
        _bit_shuffle_decoder = std::make_unique<BitShuffleDataDecoder>(DATA_PAGE);
        _bit_shuffle_decoder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
    }
    ~BinaryDictDataDecoder() override = default;

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override {
        // When the dictionary page is not full, the header of the binary dictionary's data
        // page is DICT_ENCODING, and bitshuffle decode is needed at this point. When the
        // dictionary page is full, the header of the binary dictionary's data page is PLAIN_ENCODING.
        // For the newly introduced dictionary data page, the header is BIT_SHUFFLE.
        size_t type = decode_fixed32_le((const uint8_t*)&(page_slice->data[0]));
        if (type == DICT_ENCODING || type == BIT_SHUFFLE) {
            return _bit_shuffle_decoder->decode_page_data(footer, footer_size, encoding, page, page_slice);
        } else if (type == PLAIN_ENCODING) {
            return Status::OK();
        } else {
            LOG(WARNING) << "invalid encoding type:" << type;
            return Status::Corruption(strings::Substitute("invalid encoding type:$0", type));
        }
    }

private:
    std::unique_ptr<BitShuffleDataDecoder> _bit_shuffle_decoder;
};

static DataDecoder g_base_decoder;
static BitShuffleDataDecoder g_bit_shuffle_decoder(DATA_PAGE);
static BinaryDictDataDecoder g_binary_dict_decoder;
static DictDictDecoder g_dict_dict_decoder;

DataDecoder* DataDecoder::get_data_decoder(EncodingTypePB encoding) {
    switch (encoding) {
    case BIT_SHUFFLE: {
        return &g_bit_shuffle_decoder;
    }
    case DICT_ENCODING: {
        return &g_binary_dict_decoder;
    }
    case FOR_ENCODING:
    case PLAIN_ENCODING:
    case PREFIX_ENCODING:
    case RLE: {
        return &g_base_decoder;
    }
    default: {
        return nullptr;
    }
    }
}

// For dictionary-type data pages, there are two scenarios. One is PLAIN encoding,
// and for PLAIN encoding, no additional decompression is required. The other is
// BITSHUFFLE, and in this case, pre-decompression of the page data is needed. For
// dictionary-type dictionary pages, there used to be only one type of page encoded
// with PLAIN, so no additional operation was needed. However, in this PR, we encode
// dictionary data pages with BITSHUFFLE, so pre-decompression is needed in this case.
// BITSHUFFLE encoding pages for data pages have a reserved header for recording the
// encoding type. Still, BITSHUFFLE encoding pages for dictionary pages do not have a
// reserved header.
Status StoragePageDecoder::decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                                       std::unique_ptr<char[]>* page, Slice* page_slice) {
    DCHECK(footer->has_type()) << "type must be set";
    switch (footer->type()) {
    case INDEX_PAGE:
    case SHORT_KEY_PAGE: {
        return Status::OK();
    }
    case DICTIONARY_PAGE:
        DCHECK(footer->has_dict_page_footer());
        if (footer->dict_page_footer().encoding() == PLAIN_ENCODING) {
            return Status::OK();
        }
        DCHECK(footer->dict_page_footer().encoding() == BIT_SHUFFLE);
        return g_dict_dict_decoder.decode_page_data(footer, footer_size, encoding, page, page_slice);
    case DATA_PAGE: {
        DataDecoder* decoder = DataDecoder::get_data_decoder(encoding);
        if (decoder == nullptr) {
            std::stringstream ss;
            ss << "Unknown encoding, encoding type is " << encoding;
            return Status::InternalError(ss.str());
        }
        return decoder->decode_page_data(footer, footer_size, encoding, page, page_slice);
    }
    default: {
        std::stringstream ss;
        ss << "Unknown page type, page type is " << footer->type();
        return Status::InternalError(ss.str());
    }
    }
}

} // namespace starrocks
