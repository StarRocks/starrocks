// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "storage/rowset/segment_v2/common.h"
#include "util/slice.h"


namespace starrocks {

namespace segment_v2 {

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

class BitShuffleDataDecoder : public DataDecoder {
public:
    BitShuffleDataDecoder() = default;
    ~BitShuffleDataDecoder() = default;

    void reserve_head(uint8_t head_size) override {
        DCHECK(_reserve_head_size == 0);
        _reserve_head_size = head_size;
    }
    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override;

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

    Status decode_page_data(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                            std::unique_ptr<char[]>* page, Slice* page_slice) override;

private:
    std::unique_ptr<BitShuffleDataDecoder> _bit_shuffle_decoder;
};

class StoragePageDecoder {
public:
    static Status decode_page(PageFooterPB* footer, uint32_t footer_size, EncodingTypePB encoding,
                              std::unique_ptr<char[]>* page, Slice* page_slice);
};

} // namespace segment_v2
} // namespace starrocks