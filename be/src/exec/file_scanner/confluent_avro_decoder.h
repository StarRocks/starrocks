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

#include <avrocpp/Generic.hh>
#include <avrocpp/ValidSchema.hh>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/statusor.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "libserdes/serdes.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

// Decodes one Confluent-wire-format Avro message (magic byte 0x00 + 4-byte big-endian schema id +
// raw Avro datum) into an avro::GenericDatum, resolving the writer schema by id from the Confluent
// Schema Registry via libserdes.
//
// The compiled ValidSchema is cached per schema id. Schema ids are immutable, so the cache never
// needs invalidation. libserdes already caches the raw registry schema; the only thing cached here
// is the JSON->ValidSchema compilation.
class ConfluentAvroDecoder {
public:
    // Production: resolve schemas from the Confluent Schema Registry at `schema_registry_url`.
    // init() must be called before decode().
    explicit ConfluentAvroDecoder(std::string schema_registry_url);

    // Test: a single fixed writer schema, no registry/serdes. decode() then treats the input as a
    // raw Avro datum without Confluent framing.
    explicit ConfluentAvroDecoder(avro::ValidSchema test_schema);

    ~ConfluentAvroDecoder();

    ConfluentAvroDecoder(const ConfluentAvroDecoder&) = delete;
    ConfluentAvroDecoder& operator=(const ConfluentAvroDecoder&) = delete;

    // Creates the serdes handle (production mode); no-op in test mode.
    Status init();

    // Decodes one message into `datum`. `datum` may be reused across calls by the caller.
    // If `schema_id` is non-null it receives the Confluent schema id of the message (or -1 in
    // test mode, which has no framing). Any framing/registry/decode failure returns InternalError.
    Status decode(const uint8_t* data, size_t size, avro::GenericDatum* datum, int32_t* schema_id = nullptr);

private:
    // Returns the compiled ValidSchema for `schema_id`, compiling+caching it from `serdes_schema`
    // on first use. May throw avro::Exception on malformed schema JSON (caught by decode()).
    StatusOr<const avro::ValidSchema*> _resolve(int32_t schema_id, serdes_schema_t* serdes_schema);

    const bool _test_mode;
    std::string _registry_url;
    serdes_t* _serdes = nullptr;
    std::unordered_map<int32_t, avro::ValidSchema> _schema_cache;
    avro::ValidSchema _test_schema;
    char _err_buf[512];
};

} // namespace starrocks
