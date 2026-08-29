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

#include "base/coding.h"
#include "base/container/raw_container.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/raw_container_checked.h"
#include "storage/rowset/alp_page.h"
#include "storage/rowset/bitshuffle_wrapper.h"

namespace starrocks {

class BitShuffleDataDecoder : public DataDecoder {
public:
    BitShuffleDataDecoder() = default;
    ~BitShuffleDataDecoder() override = default;

    void reserve_head(uint8_t head_size) override {
        DCHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }
    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<std::vector<uint8_t>>* page, Slice* page_slice) override {
        size_t num_elements = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 0);
        size_t compressed_size = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 4);
        size_t num_element_after_padding = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 8);
        size_t size_of_element = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 12);
        DCHECK_EQ(num_element_after_padding, ALIGN_UP(num_elements, 8U));

        size_t header_size = _reserve_head_size + BITSHUFFLE_PAGE_HEADER_SIZE;
        size_t data_size = num_element_after_padding * size_of_element;

        // data_size is size of decoded_data
        // compressed_size contains encoded_data size and BITSHUFFLE_PAGE_HEADER_SIZE
        std::unique_ptr<std::vector<uint8_t>> decompressed_page(new std::vector<uint8_t>());
        size_t new_size = page_slice->size + data_size - (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE);
        RETURN_IF_ERROR(raw::stl_vector_resize_uninitialized_checked(decompressed_page.get(), new_size));
        memcpy(decompressed_page.get()->data(), page_slice->data, header_size);

        Slice compressed_body(page_slice->data + header_size, compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE);
        Slice decompressed_body(decompressed_page->data() + header_size, data_size);
        int64_t bytes = bitshuffle::decompress_lz4(compressed_body.data, decompressed_body.data,
                                                   num_element_after_padding, size_of_element, 0);
        if (bytes != compressed_body.size) {
            return Status::Corruption(
                    strings::Substitute("decompress failed: expected number of bytes consumed=$0 vs real consumed=$1",
                                        compressed_body.size, bytes));
        }
        DCHECK(footer->has_type()) << "type must be set";
        uint32_t null_size = 0;
        if (footer->type() == DATA_PAGE) {
            const DataPageFooterPB& data_footer = footer->data_page_footer();
            null_size = data_footer.nullmap_size();
        }
        memcpy(decompressed_body.data + decompressed_body.size,
               page_slice->data + header_size + (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE),
               null_size + footer_size);

        *page = std::move(decompressed_page);
        *page_slice = Slice((*page)->data(), header_size + data_size + null_size + footer_size);

        return Status::OK();
    }

private:
    uint8_t _reserve_head_size = 0;
};

class DictDictDecoder : public DataDecoder {
public:
    DictDictDecoder() { _bit_shuffle_decoder = std::make_unique<BitShuffleDataDecoder>(); }
    ~DictDictDecoder() override = default;

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<std::vector<uint8_t>>* page, Slice* page_slice) override {
        return _bit_shuffle_decoder->decode_page_data(footer, footer_size, encoding, page, page_slice);
    }

private:
    std::unique_ptr<BitShuffleDataDecoder> _bit_shuffle_decoder;
};

class BinaryDictDataDecoder : public DataDecoder {
public:
    BinaryDictDataDecoder() {
        _bit_shuffle_decoder = std::make_unique<BitShuffleDataDecoder>();
        _bit_shuffle_decoder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
    }
    ~BinaryDictDataDecoder() override = default;

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<std::vector<uint8_t>>* page, Slice* page_slice) override {
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

// Fully decodes an ALP_ENCODING data page at page-load time, mirroring
// BitShuffleDataDecoder: the page cache then holds raw float/double values
// and AlpPageDecoder is a plain memcpy decoder.
class AlpDataDecoder : public DataDecoder {
public:
    AlpDataDecoder() = default;
    ~AlpDataDecoder() override = default;

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<std::vector<uint8_t>>* page, Slice* page_slice) override {
        if (page_slice->size < ALP_PAGE_HEADER_SIZE) {
            return Status::Corruption(strings::Substitute("invalid ALP page size:$0", page_slice->size));
        }
        size_t num_elements = decode_fixed32_le((const uint8_t*)page_slice->data + 0);
        size_t encoded_size = decode_fixed32_le((const uint8_t*)page_slice->data + 4);
        size_t num_padded = decode_fixed32_le((const uint8_t*)page_slice->data + 8);
        size_t size_of_element = decode_fixed32_le((const uint8_t*)page_slice->data + 12);
        if (size_of_element != 4 && size_of_element != 8) {
            return Status::Corruption(strings::Substitute("invalid ALP size_of_element:$0", size_of_element));
        }
        if (num_padded != alppage::padded_element_count(num_elements)) {
            return Status::Corruption(
                    strings::Substitute("ALP element count corrupted, padded:$0, num:$1", num_padded, num_elements));
        }
        if (encoded_size < ALP_PAGE_HEADER_SIZE || encoded_size > page_slice->size) {
            return Status::Corruption(
                    strings::Substitute("invalid ALP encoded size:$0, page size:$1", encoded_size, page_slice->size));
        }
        // Every 1024-value vector costs at least its meta bytes in the encoded
        // body, which bounds the decoded size a valid page can claim; reject
        // implausible counts before they drive the allocation below. (The
        // remaining ratio is legitimate: a constant column stores 1024 values
        // in one 16-byte meta.)
        if (num_padded / ALP_PAGE_VECTOR_SIZE * ALP_PAGE_VECTOR_META_SIZE > encoded_size - ALP_PAGE_HEADER_SIZE) {
            return Status::Corruption(strings::Substitute("implausible ALP element count:$0 for encoded size:$1",
                                                          num_padded, encoded_size));
        }
        // The writer caps a page's raw bytes by data_page_size, an int32
        // config, so no legitimate page can decode past INT32_MAX plus one
        // vector of padding regardless of the writer's configuration.
        if (num_padded * size_of_element > (size_t)INT32_MAX + ALP_PAGE_VECTOR_SIZE * size_of_element) {
            return Status::Corruption(
                    strings::Substitute("implausible ALP decoded size:$0", num_padded * size_of_element));
        }

        size_t header_size = ALP_PAGE_HEADER_SIZE;
        // Retain only the real values: the tail vector's padding is decoded
        // into scratch space inside alp_decode_body and dropped, so partial
        // pages do not waste page-cache memory.
        size_t data_size = num_elements * size_of_element;

        std::unique_ptr<std::vector<uint8_t>> decoded_page(new std::vector<uint8_t>());
        size_t new_size = page_slice->size + data_size - (encoded_size - header_size);
        RETURN_IF_ERROR(raw::stl_vector_resize_uninitialized_checked(decoded_page.get(), new_size));
        memcpy(decoded_page->data(), page_slice->data, header_size);

        const uint8_t* body = (const uint8_t*)page_slice->data + header_size;
        size_t body_size = encoded_size - header_size;
        if (size_of_element == 4) {
            RETURN_IF_ERROR(
                    alppage::alp_decode_body<float>(body, body_size, num_padded, num_elements,
                                                    reinterpret_cast<float*>(decoded_page->data() + header_size)));
        } else {
            RETURN_IF_ERROR(
                    alppage::alp_decode_body<double>(body, body_size, num_padded, num_elements,
                                                     reinterpret_cast<double*>(decoded_page->data() + header_size)));
        }

        DCHECK(footer->has_type()) << "type must be set";
        uint32_t null_size = 0;
        if (footer->type() == DATA_PAGE) {
            null_size = footer->data_page_footer().nullmap_size();
        }
        // The trailer sizes come from the page footer; a malformed page could
        // claim more trailer bytes than the input actually holds (reading past
        // page_slice and writing past decoded_page below), or fewer, leaving
        // surplus bytes inside the cached page whose footer is parsed from its
        // very end. The input must be consumed exactly.
        if (page_slice->size - encoded_size != (size_t)null_size + footer_size) {
            return Status::Corruption(
                    strings::Substitute("ALP page trailer mismatch, page size:$0, encoded size:$1, trailer size:$2",
                                        page_slice->size, encoded_size, (size_t)null_size + footer_size));
        }
        memcpy(decoded_page->data() + header_size + data_size, page_slice->data + encoded_size,
               null_size + footer_size);

        *page = std::move(decoded_page);
        *page_slice = Slice((*page)->data(), header_size + data_size + null_size + footer_size);
        return Status::OK();
    }
};

static DataDecoder g_base_decoder;
static BitShuffleDataDecoder g_bit_shuffle_decoder;
static BinaryDictDataDecoder g_binary_dict_decoder;
static DictDictDecoder g_dict_dict_decoder;
static AlpDataDecoder g_alp_data_decoder;

DataDecoder* DataDecoder::get_data_decoder(EncodingTypePB encoding) {
    switch (encoding) {
    case BIT_SHUFFLE: {
        return &g_bit_shuffle_decoder;
    }
    case ALP_ENCODING: {
        return &g_alp_data_decoder;
    }
    case DICT_ENCODING: {
        return &g_binary_dict_decoder;
    }
    case FOR_ENCODING:
    case PLAIN_ENCODING:
    case PLAIN_ENCODING_DELTA_OFFSET:
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
                                       std::unique_ptr<std::vector<uint8_t>>* page, Slice* page_slice) {
    DCHECK(footer->has_type()) << "type must be set";
    switch (footer->type()) {
    case INDEX_PAGE:
    case SHORT_KEY_PAGE:
    case SORT_KEY_PAGE: {
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
