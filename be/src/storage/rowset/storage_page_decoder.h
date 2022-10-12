// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "storage/rowset/common.h"
#include "util/slice.h"

namespace starrocks {

class DataDecoder {
public:
    DataDecoder() = default;
    virtual ~DataDecoder() = default;

    static DataDecoder* get_data_decoder(EncodingTypePB encoding);

    virtual void reserve_head(uint8_t head_size) {}

    virtual Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                                    std::unique_ptr<char[]>* page, Slice* page_slice) {
        return Status::OK();
    }
};

class StoragePageDecoder {
public:
    static Status decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                              std::unique_ptr<char[]>* page, Slice* page_slice);
};

} // namespace starrocks
