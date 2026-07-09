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
    if (type == ByteBufferMetaType::NONE) {
        return "NONE";
    } else if (type == ByteBufferMetaType::KAFKA) {
        return "KAFKA";
    } else if (type == ByteBufferMetaType::PULSAR) {
        return "PULSAR";
    } else {
        return "UNKNOWN";
    }
}

StatusOr<ByteBufferMeta*> ByteBufferMeta::create(ByteBufferMetaType meta_type) {
    switch (meta_type) {
    case ByteBufferMetaType::NONE:
        return NoneByteBufferMeta::instance();
    case ByteBufferMetaType::KAFKA:
    case ByteBufferMetaType::PULSAR:
        return new StreamMessageMeta(meta_type);
    }
    return Status::NotSupported(fmt::format("unknown byte buffer meta type {}", byte_buffer_meta_type_name(meta_type)));
}

Status NoneByteBufferMeta::copy_from(ByteBufferMeta* source) {
    if (source != this) {
        return Status::NotSupported(fmt::format("can't copy byte buffer {} meta to {} meta",
                                                byte_buffer_meta_type_name(source->type()),
                                                byte_buffer_meta_type_name(type())));
    }
    return Status::OK();
}

Status StreamMessageMeta::copy_from(ByteBufferMeta* source) {
    if (source->type() != _source) {
        return Status::NotSupported(fmt::format("can't copy byte buffer {} meta to {} meta",
                                                byte_buffer_meta_type_name(source->type()),
                                                byte_buffer_meta_type_name(type())));
    }
    auto* meta = static_cast<StreamMessageMeta*>(source);
    // Overwrite every field; never append. A negative/empty/!has_key value means "absent" and must
    // replace any stale data from the message this buffer previously held.
    _topic = meta->_topic;
    _partition = meta->_partition;
    _offset = meta->_offset;
    _message_id = meta->_message_id;
    _timestamp = meta->_timestamp;
    _event_timestamp = meta->_event_timestamp;
    _has_key = meta->_has_key;
    _key = meta->_has_key ? meta->_key : std::string();
    _headers = meta->_headers;
    return Status::OK();
}

std::string StreamMessageMeta::to_string() const {
    if (_source == ByteBufferMetaType::PULSAR) {
        return fmt::format("pulsar topic: {}, partition: {}, message_id: {}", _topic, _partition, _message_id);
    }
    if (_topic.empty()) {
        // A Kafka buffer from a job with no metadata column carries only partition/offset.
        return fmt::format("kafka partition: {}, offset: {}", _partition, _offset);
    }
    return fmt::format("kafka topic: {}, partition: {}, offset: {}", _topic, _partition, _offset);
}

} // namespace starrocks
