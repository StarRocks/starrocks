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
