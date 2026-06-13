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

#include "exec/file_scanner/confluent_avro_decoder.h"

#include <fmt/format.h>

#include <avrocpp/Compiler.hh>
#include <avrocpp/Decoder.hh>
#include <avrocpp/Exception.hh>
#include <avrocpp/Stream.hh>

namespace starrocks {

ConfluentAvroDecoder::ConfluentAvroDecoder(std::string schema_registry_url)
        : _test_mode(false), _registry_url(std::move(schema_registry_url)) {}

ConfluentAvroDecoder::ConfluentAvroDecoder(avro::ValidSchema test_schema)
        : _test_mode(true), _test_schema(std::move(test_schema)) {}

ConfluentAvroDecoder::~ConfluentAvroDecoder() {
    if (_serdes != nullptr) {
        serdes_destroy(_serdes);
        _serdes = nullptr;
    }
}

Status ConfluentAvroDecoder::init() {
    if (_test_mode) {
        return Status::OK();
    }
    // serdes_new takes ownership of the conf object.
    serdes_conf_t* conf = serdes_conf_new(nullptr, 0, "schema.registry.url", _registry_url.c_str(), NULL);
    _serdes = serdes_new(conf, _err_buf, sizeof(_err_buf));
    if (_serdes == nullptr) {
        return Status::InternalError(fmt::format("failed to create serdes handle: {}", _err_buf));
    }
    return Status::OK();
}

StatusOr<const avro::ValidSchema*> ConfluentAvroDecoder::_resolve(int32_t schema_id, serdes_schema_t* serdes_schema) {
    if (auto it = _schema_cache.find(schema_id); it != _schema_cache.end()) {
        return &it->second;
    }
    const char* definition = serdes_schema_definition(serdes_schema);
    if (definition == nullptr) {
        return Status::InternalError(fmt::format("empty schema definition for schema id {}", schema_id));
    }
    // compileJsonSchemaFromString may throw avro::Exception on malformed JSON; decode() catches it.
    avro::ValidSchema valid_schema = avro::compileJsonSchemaFromString(definition);
    auto [it, _] = _schema_cache.emplace(schema_id, std::move(valid_schema));
    return &it->second;
}

Status ConfluentAvroDecoder::decode(const uint8_t* data, size_t size, avro::GenericDatum* datum, int32_t* schema_id) {
    try {
        const avro::ValidSchema* schema = nullptr;
        const uint8_t* payload = data;
        size_t payload_size = size;

        if (_test_mode) {
            // Input is a raw Avro datum without Confluent framing.
            schema = &_test_schema;
            if (schema_id != nullptr) {
                *schema_id = -1;
            }
        } else {
            // serdes_framing_read strips the Confluent framing per the serdes config, advances the
            // payload pointer/length past it, and resolves the writer schema (registry-cached).
            const void* p = data;
            size_t sz = size;
            serdes_schema_t* serdes_schema = nullptr;
            ssize_t framing = serdes_framing_read(_serdes, &p, &sz, &serdes_schema, _err_buf, sizeof(_err_buf));
            if (framing == -1 || serdes_schema == nullptr) {
                return Status::InternalError(fmt::format("confluent framing/schema resolve failed: {}", _err_buf));
            }
            payload = static_cast<const uint8_t*>(p);
            payload_size = sz;
            int32_t resolved_id = serdes_schema_id(serdes_schema);
            if (schema_id != nullptr) {
                *schema_id = resolved_id;
            }
            ASSIGN_OR_RETURN(schema, _resolve(resolved_id, serdes_schema));
        }

        auto input = avro::memoryInputStream(payload, payload_size);
        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*input);
        avro::GenericReader::read(*decoder, *datum, *schema);
        return Status::OK();
    } catch (const avro::Exception& e) {
        return Status::InternalError(fmt::format("avro decode failed: {}", e.what()));
    }
}

} // namespace starrocks
