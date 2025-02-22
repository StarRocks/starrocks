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

#include "byte_buffer.h"

namespace starrocks {

std::string byte_buffer_meta_type_name(ByteBufferMetaType type) {
    switch (type) {
    case ByteBufferMetaType::NONE:
        return "NONE";
    case ByteBufferMetaType::KAFKA:
        return "KAFKA";
    default:
        return "UNKNOWN";
    }
}

StatusOr<ByteBufferMeta*> ByteBufferMeta::create(ByteBufferMetaType meta_type) {
    switch (meta_type) {
    case ByteBufferMetaType::NONE:
        return NoneByteBufferMeta::instance();
    case ByteBufferMetaType::KAFKA:
        return new KafkaByteBufferMeta();
    }
    return Status::NotSupported(fmt::format("unknown byte buffer meta type {}", byte_buffer_meta_type_name(meta_type)));
}

} // namespace starrocks