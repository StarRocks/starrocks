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

#include "base/bit/bit_util.h"
#include "base/coding.h"
#include "base/container/raw_container.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/raw_container_checked.h"
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
        size_t header_size = _reserve_head_size + BITSHUFFLE_PAGE_HEADER_SIZE;
        // All the sizes below come straight from the (possibly corrupted) page
        // bytes, so validate them before they drive any allocation or memcpy.
        if (page_slice->size < header_size) {
            return Status::Corruption(strings::Substitute("invalid bitshuffle page size:$0, header size:$1",
                                                          page_slice->size, header_size));
        }
        size_t num_elements = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 0);
        size_t compressed_size = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 4);
        size_t num_element_after_padding = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 8);
        size_t size_of_element = decode_fixed32_le((const uint8_t*)page_slice->data + _reserve_head_size + 12);
        // Not ALIGN_UP(): its mask is 32-bit, so a corrupted num_elements near
        // 2^32 wraps -- ALIGN_UP(0xffffffff, 8U) is 0 and would match a crafted
        // padded count of 0. RoundUpToPowerOf2() rounds at full 64-bit width.
        size_t expected_padded = static_cast<size_t>(BitUtil::RoundUpToPowerOf2(num_elements, 8));
        if (num_element_after_padding != expected_padded) {
            return Status::Corruption(strings::Substitute("bitshuffle element count corrupted, padded:$0, num:$1",
                                                          num_element_after_padding, num_elements));
        }
        if (compressed_size < BITSHUFFLE_PAGE_HEADER_SIZE || compressed_size > page_slice->size - _reserve_head_size) {
            return Status::Corruption(strings::Substitute("invalid bitshuffle compressed size:$0, page size:$1",
                                                          compressed_size, page_slice->size));
        }
        // Same element sizes BitShufflePageDecoder::init accepts; anything else
        // (e.g. 0xffffffff) would drive an absurd allocation below.
        switch (size_of_element) {
        case 1:
        case 2:
        case 3:
        case 4:
        case 8:
        case 12:
        case 16:
        case 32:
            break;
        default:
            return Status::Corruption(strings::Substitute("invalid bitshuffle size_of_elem:$0", size_of_element));
        }

        size_t data_size = num_element_after_padding * size_of_element;
        // LZ4 cannot expand beyond ~255x, so a decoded size far past the
        // compressed body is corrupt; reject it before allocating (load paths
        // may run with memory-limit enforcement disabled).
        if (data_size > 256 * (compressed_size - BITSHUFFLE_PAGE_HEADER_SIZE) + 4096) {
            return Status::Corruption(strings::Substitute(
                    "implausible bitshuffle decoded size:$0 for compressed size:$1", data_size, compressed_size));
        }

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
        // The trailer sizes come from the page footer; a malformed page could
        // claim more trailer bytes than the input actually holds (reading past
        // page_slice and writing past decompressed_page below), or fewer,
        // leaving surplus bytes that end up inside the cached page whose footer
        // is parsed from its very end. The input must be consumed exactly.
        if (page_slice->size - _reserve_head_size - compressed_size != (size_t)null_size + footer_size) {
            return Status::Corruption(strings::Substitute(
                    "bitshuffle page trailer mismatch, page size:$0, compressed size:$1, trailer size:$2",
                    page_slice->size, compressed_size, (size_t)null_size + footer_size));
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

static DataDecoder g_base_decoder;
static BitShuffleDataDecoder g_bit_shuffle_decoder;
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
