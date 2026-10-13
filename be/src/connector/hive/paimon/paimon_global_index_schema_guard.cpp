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

#include "connector/hive/paimon/paimon_global_index_schema_guard.h"

#include <paimon/io/byte_array_input_stream.h>
#include <rapidjson/document.h>

namespace starrocks {
namespace {

class ValidatedSchemaInputStream final : public paimon::ByteArrayInputStream {
public:
    ValidatedSchemaInputStream(std::shared_ptr<const std::string> contents, std::string uri)
            : paimon::ByteArrayInputStream(contents->data(), contents->size()),
              _contents(std::move(contents)),
              _uri(std::move(uri)) {}
    paimon::Result<std::string> GetUri() const override { return _uri; }

private:
    // ByteArrayInputStream borrows its buffer. Keep the validated bytes alive, and
    // never re-read the underlying file after checking it (including async reads).
    std::shared_ptr<const std::string> _contents;
    std::string _uri;
};

} // namespace

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonGlobalIndexSchemaGuard::Open(const std::string& path) const {
    if (!path.starts_with(_schema_prefix)) {
        return _delegate->Open(path);
    }
    if (path != _schema_prefix + std::to_string(_schema_id)) {
        return paimon::Status::Invalid("Paimon Global Index schema changed after planning");
    }
    auto opened = _delegate->Open(path);
    if (!opened.ok()) {
        return opened.status();
    }
    auto input = std::move(opened).value();
    auto length = input->Length();
    if (!length.ok()) {
        return length.status();
    }
    // This only disables an optional optimization for unusually large metadata.
    constexpr int64_t kMaxSchemaBytes = 16 * 1024 * 1024;
    if (length.value() <= 0 || length.value() > kMaxSchemaBytes) {
        return paimon::Status::Invalid("Invalid Paimon Global Index schema size");
    }
    auto contents = std::make_shared<std::string>(length.value(), '\0');
    int64_t offset = 0;
    while (offset < length.value()) {
        auto read = input->Read(contents->data() + offset, length.value() - offset);
        if (!read.ok()) {
            return read.status();
        }
        if (read.value() <= 0 || read.value() > length.value() - offset) {
            return paimon::Status::Invalid("Truncated Paimon Global Index schema");
        }
        offset += read.value();
    }
    auto closed = input->Close();
    if (!closed.ok()) {
        return closed;
    }
    rapidjson::Document schema;
    if (schema.Parse(contents->data(), contents->size()).HasParseError() || !schema.IsObject() ||
        !schema.HasMember("id") || !schema["id"].IsInt64() || schema["id"].GetInt64() != _schema_id ||
        !schema.HasMember("timeMillis") || !schema["timeMillis"].IsInt64() ||
        schema["timeMillis"].GetInt64() != _schema_time_millis) {
        return paimon::Status::Invalid("Paimon Global Index schema identity does not match planning");
    }
    _schema_validated.store(true);
    return std::make_unique<ValidatedSchemaInputStream>(std::move(contents), path);
}

} // namespace starrocks
