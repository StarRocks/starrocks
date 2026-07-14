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

#include "formats/puffin/puffin_writer.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "fs/fs.h"
#include "gutil/endian.h"

namespace starrocks::formats {

static const char kPuffinMagic[4] = {0x50, 0x46, 0x41, 0x31}; // "PFA1"

Status PuffinWriter::init() {
    if (_initialized) {
        return Status::InternalError("PuffinWriter::init called more than once");
    }
    RETURN_IF_ERROR(_file->append(Slice(kPuffinMagic, sizeof(kPuffinMagic))));
    _offset = sizeof(kPuffinMagic);
    _initialized = true;
    return Status::OK();
}

StatusOr<PuffinBlobMetadata> PuffinWriter::add_blob(const std::string& type, const std::vector<int32_t>& input_fields,
                                                    const std::map<std::string, std::string>& properties,
                                                    const uint8_t* data, size_t size) {
    if (!_initialized) {
        return Status::InternalError("PuffinWriter::add_blob called before init");
    }
    if (_finished) {
        return Status::InternalError("PuffinWriter::add_blob called after finish");
    }
    PuffinBlobMetadata meta;
    meta.type = type;
    meta.input_fields = input_fields;
    meta.properties = properties;
    meta.offset = _offset;
    meta.length = static_cast<int64_t>(size);

    RETURN_IF_ERROR(_file->append(Slice(reinterpret_cast<const char*>(data), size)));
    _offset += static_cast<int64_t>(size);
    _blobs.push_back(meta);
    return meta;
}

Status PuffinWriter::finish() {
    if (!_initialized) {
        return Status::InternalError("PuffinWriter::finish called before init");
    }
    if (_finished) {
        return Status::InternalError("PuffinWriter::finish called more than once");
    }
    // Mark finished up front so a retry after a partial-append failure cannot
    // append a *second* footer on top of the partial one. This does NOT make
    // finish() atomic: on I/O failure a partial footer may remain, so the caller
    // must discard the output file rather than reference it.
    _finished = true;

    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> w(sb);
    w.StartObject();
    w.Key("blobs");
    w.StartArray();
    for (const auto& b : _blobs) {
        w.StartObject();
        w.Key("type");
        w.String(b.type.c_str());
        w.Key("fields");
        w.StartArray();
        for (int32_t f : b.input_fields) w.Int(f);
        w.EndArray();
        w.Key("snapshot-id");
        w.Int64(-1);
        w.Key("sequence-number");
        w.Int64(-1);
        w.Key("offset");
        w.Int64(b.offset);
        w.Key("length");
        w.Int64(b.length);
        if (!b.properties.empty()) {
            w.Key("properties");
            w.StartObject();
            for (const auto& [k, v] : b.properties) {
                w.Key(k.c_str());
                w.String(v.c_str());
            }
            w.EndObject();
        }
        w.EndObject();
    }
    w.EndArray();
    w.EndObject();

    const char* payload = sb.GetString();
    const auto payload_size = static_cast<uint32_t>(sb.GetSize());

    // footer: magic | payload | payloadSize(LE) | flags(4B=0) | magic
    RETURN_IF_ERROR(_file->append(Slice(kPuffinMagic, sizeof(kPuffinMagic))));
    RETURN_IF_ERROR(_file->append(Slice(payload, payload_size)));

    uint8_t size_le[4];
    LittleEndian::Store32(size_le, payload_size);
    RETURN_IF_ERROR(_file->append(Slice(reinterpret_cast<const char*>(size_le), 4)));

    const uint8_t flags[4] = {0, 0, 0, 0};
    RETURN_IF_ERROR(_file->append(Slice(reinterpret_cast<const char*>(flags), 4)));

    RETURN_IF_ERROR(_file->append(Slice(kPuffinMagic, sizeof(kPuffinMagic))));
    return Status::OK();
}

} // namespace starrocks::formats
