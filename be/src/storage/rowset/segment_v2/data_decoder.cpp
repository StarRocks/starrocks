// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset/segment_v2/data_decoder.h"

namespace starrocks::segment_v2 {

std::unique_ptr<DataDecoder> get_data_decoder(EncodingTypePB encoding) {
    switch (encoding) {
    case BIT_SHUFFLE: {
        return std::make_unique<BitShuffleDecoder>();
    }
    case FOR_ENCODING: {
        return std::make_unique<FrameOfReferenceDecoder>();
    }
    case PLAIN_ENCODING: {
        return std::make_unique<BinaryPlainDecoder>();
    }
    case DICT_ENCODING: {
        return std::make_unique<BinaryDictDecoder>();
    }
    case PREFIX_ENCODING: {
        return std::make_unique<BinaryPrefixDecoder>();
    }
    case RLE: {
        return std::make_unique<RleDecoder>();
    }
    default: {
        return nullptr;
    }
    }
}

Status DataDecoder::decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                                std::unique_ptr<char[]>* page, Slice& page_slice) {
    CHECK(footer->has_type()) << "type must be set";
    switch (footer->type()) {
    case INDEX_PAGE:
    case DICTIONARY_PAGE:
    case SHORT_KEY_PAGE: {
        return Status::OK();
    }
    case DATA_PAGE: {
        std::unique_ptr<DataDecoder> decoder = get_data_decoder(encoding);
        if (!decoder) {
            return Status::InternalError("Unknown encoding");
        }
        return decoder->decode_data_page(footer, footer_size, encoding, page, page_slice);
    }
    default: {
        return Status::OK();
    }
    }
}

} // namespace starrocks::segment_v2
