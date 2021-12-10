// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "storage/rowset/segment_v2/storage_page_data_decoder.h"

namespace starrocks {

namespace segment_v2 {

class StoragePageDecoder {
public:
    StoragePageDecoder();
    virtual ~StoragePageDecoder();

    static StoragePageDecoder* instance() { return _s_instance; }

    static void create_global_storage_page_decoder();

    std::unique_ptr<DataDecoder>* get_data_decoder(EncodingTypePB encoding);

    Status decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                       std::unique_ptr<char[]>* page, Slice* page_slice);

private:
    static StoragePageDecoder* _s_instance;

    // Decode is required only when page is encoded as bitshuffle and dict
    std::unique_ptr<DataDecoder> _base_decoder = nullptr;
    std::unique_ptr<DataDecoder> _bit_shuffle_decoder = nullptr;
    std::unique_ptr<DataDecoder> _binary_dict_decoder = nullptr;
};

} // namespace segment_v2
} // namespace starrocks