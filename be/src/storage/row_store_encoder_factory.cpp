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

#include "storage/row_store_encoder_factory.h"

namespace starrocks {

RowStoreEncoderFactory::RowStoreEncoderFactory() = default;
RowStoreEncoderFactory::~RowStoreEncoderFactory() = default;

RowStoreEncoderPtr RowStoreEncoderFactory::get_or_create_encoder(const RowStoreEncoderType& encoder_type) {
    std::lock_guard guard(_mutex);
    uint8_t type = encoder_type;
    auto iter = _rowstore_encoders.find(type);
    if (iter != _rowstore_encoders.end()) {
        return iter->second;
    }
    RowStoreEncoderPtr encoder;
    switch (encoder_type) {
    case SIMPLE:
        encoder = std::make_shared<RowStoreEncoderSimple>();
        break;
    default:
        break;
    }

    _rowstore_encoders.emplace(type, encoder);
    return encoder;
}

} // namespace starrocks