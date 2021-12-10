// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset/segment_v2/storage_page_decoder.h"

namespace starrocks::segment_v2 {

StoragePageDecoder* StoragePageDecoder::_s_instance = nullptr;

void StoragePageDecoder::create_global_storage_page_decoder() {
    if (_s_instance == nullptr) {
        _s_instance = new StoragePageDecoder();
    }
}

StoragePageDecoder::StoragePageDecoder() {
    if (_base_decoder == nullptr) {
        _base_decoder = std::make_unique<DataDecoder>();
    }

    if (_bit_shuffle_decoder == nullptr) {
        _bit_shuffle_decoder = std::make_unique<BitShuffleDecoder>();
    } 

    if (_binary_dict_decoder == nullptr) {
        _binary_dict_decoder = std::make_unique<BinaryDictDecoder>();
    }
}


StoragePageDecoder::~StoragePageDecoder() {}

std::unique_ptr<DataDecoder>* StoragePageDecoder::get_data_decoder(EncodingTypePB encoding) {
    switch (encoding) {
    case BIT_SHUFFLE: {
        return &_bit_shuffle_decoder;
    }
    case DICT_ENCODING: {
        return &_binary_dict_decoder;
    }
    case FOR_ENCODING:
    case PLAIN_ENCODING:
    case PREFIX_ENCODING:
    case RLE: {
        return &_base_decoder;
    }
    default: {
        return nullptr;
    }
    }
}

Status StoragePageDecoder::decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                                std::unique_ptr<char[]>* page, Slice* page_slice) {
    DCHECK(footer->has_type()) << "type must be set";
    switch (footer->type()) {
    case INDEX_PAGE:
    case DICTIONARY_PAGE:
    case SHORT_KEY_PAGE: {
        return Status::OK();
    }
    case DATA_PAGE: {
        std::unique_ptr<DataDecoder>* decoder = get_data_decoder(encoding);
        if (*decoder == nullptr) {
            std::stringstream ss;
            ss << "Unknown encoding, encoding type is " << encoding;
            return Status::InternalError(ss.str());
        }
        return (*decoder)->decode_data_page(footer, footer_size, encoding, page, page_slice);
    }
    default: {
        std::stringstream ss;
        ss << "Unknown page type, page type is " << footer->type();
        return Status::InternalError(ss.str());
    }
    }
}

} // namespace starrocks::segment_v2
