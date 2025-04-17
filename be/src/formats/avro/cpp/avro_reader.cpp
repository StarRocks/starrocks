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

#include "formats/avro/cpp/avro_reader.h"

#include <fmt/format.h>

#include <avrocpp/NodeImpl.hh>
#include <avrocpp/Types.hh>
#include <avrocpp/ValidSchema.hh>

#include "exec/file_scanner.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks {

bool AvroBufferInputStream::next(const uint8_t** data, size_t* len) {
    if (_available == 0 && !fill()) {
        return false;
    }

    *data = _next;
    *len = _available;
    _next += _available;
    _byte_count += _available;
    _available = 0;
    return true;
}

void AvroBufferInputStream::backup(size_t len) {
    _next -= len;
    _available += len;
    _byte_count -= len;
}

void AvroBufferInputStream::skip(size_t len) {
    while (len > 0) {
        if (_available == 0) {
            auto st = _file->seek(_byte_count + len);
            if (!st.ok()) {
                throw avro::Exception(fmt::format("Avro input stream skip failed. error: {}", st.to_string()));
            }

            _byte_count += len;
            return;
        }

        size_t n = std::min(_available, len);
        _available -= n;
        _next += n;
        len -= n;
        _byte_count += n;
    }
}

void AvroBufferInputStream::seek(int64_t position) {
    auto st = _file->seek(position);
    if (!st.ok()) {
        throw avro::Exception(fmt::format("Avro input stream seek failed. error: {}", st.to_string()));
    }

    _byte_count = position;
    _available = 0;
}

bool AvroBufferInputStream::fill() {
    ++_counter->file_read_count;
    SCOPED_RAW_TIMER(&_counter->file_read_ns);
    auto ret = _file->read(_buffer, _buffer_size);
    if (!ret.ok() || ret.value() == 0) {
        return false;
    }

    _next = _buffer;
    _available = ret.value();
    return true;
}

AvroReader::~AvroReader() {
    if (_reader != nullptr) {
        _reader->close();
        _reader.reset();
    }
}

Status AvroReader::init(std::unique_ptr<avro::InputStream> input_stream) {
    try {
        _reader = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(std::move(input_stream));
        return Status::OK();
    } catch (const avro::Exception& ex) {
        auto err_msg = fmt::format("Avro reader init throws exception: {}", ex.what());
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }
}

Status AvroReader::get_schema(std::vector<SlotDescriptor>* schema) {
    if (_reader == nullptr) {
        return Status::Uninitialized("Avro reader is not initialized");
    }

    try {
        const auto& avro_schema = _reader->dataSchema();
        VLOG(2) << "avro data schema: " << avro_schema.toJson(false);

        const auto& node = avro_schema.root();
        if (node->type() != avro::AVRO_RECORD) {
            return Status::NotSupported(fmt::format("Root node is not record. type: {}", avro::toString(node->type())));
        }

        for (size_t i = 0; i < node->leaves(); ++i) {
            auto field_name = node->nameAt(i);
            auto field_node = node->leafAt(i);
            TypeDescriptor desc;
            RETURN_IF_ERROR(get_avro_type(field_node, &desc));
            schema->emplace_back(i, field_name, desc);
        }
        return Status::OK();
    } catch (const avro::Exception& ex) {
        auto err_msg = fmt::format("Avro reader get schema throws exception: {}", ex.what());
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }
}

} // namespace starrocks
