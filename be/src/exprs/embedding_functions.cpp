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

#include "exprs/embedding_functions.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "platform/http/http_client.h"
#include "types/json_value.h"

namespace starrocks {

namespace {

// Embedding provider config carried in the JSON argument. This mirrors the fields emitted by the
// FE's EmbeddingConfigJson. It intentionally differs from the chat-oriented platform/llm ModelConfig:
// api_key is OPTIONAL (local/self-hosted providers run without auth) and dimensions is honored so the
// provider returns vectors matching the fixed-width context vector column.
struct EmbeddingConfig {
    std::string endpoint;
    std::string model;
    std::string api_key; // optional; empty means no Authorization header
    int dimensions = 0;  // optional; 0 means "provider default, do not request a specific size"
    int64_t timeout_ms = 60000;
};

StatusOr<EmbeddingConfig> parse_embedding_config(const JsonValue& json) {
    rapidjson::Document doc;
    doc.Parse(json.to_string_uncheck().c_str());
    if (doc.HasParseError() || !doc.IsObject()) {
        return Status::InvalidArgument("embedding: config argument is not a JSON object");
    }
    EmbeddingConfig config;
    if (!doc.HasMember("endpoint") || !doc["endpoint"].IsString()) {
        return Status::InvalidArgument("embedding: config missing required string field 'endpoint'");
    }
    config.endpoint = doc["endpoint"].GetString();
    if (!doc.HasMember("model") || !doc["model"].IsString()) {
        return Status::InvalidArgument("embedding: config missing required string field 'model'");
    }
    config.model = doc["model"].GetString();
    // api_key is optional: no-auth local providers omit it entirely.
    if (doc.HasMember("api_key") && doc["api_key"].IsString()) {
        config.api_key = doc["api_key"].GetString();
    }
    // dimensions is optional: forward it so the provider trims/pads to the configured width.
    if (doc.HasMember("dimensions") && doc["dimensions"].IsInt()) {
        config.dimensions = doc["dimensions"].GetInt();
    }
    if (doc.HasMember("timeout_ms") && doc["timeout_ms"].IsInt64()) {
        config.timeout_ms = doc["timeout_ms"].GetInt64();
    }
    return config;
}

// Simple per-row embedding call: POST {"model": ..., "input": text[, "dimensions": N]} to the
// configured OpenAI-compatible /v1/embeddings endpoint and parse data[0].embedding into a float
// vector.
StatusOr<std::vector<float>> call_embedding(const std::string& text, const EmbeddingConfig& config) {
    rapidjson::Document req;
    req.SetObject();
    auto& allocator = req.GetAllocator();
    req.AddMember("model", rapidjson::Value(config.model.c_str(), allocator), allocator);
    req.AddMember("input", rapidjson::Value(text.c_str(), allocator), allocator);
    // Forward the configured vector width so the returned embedding matches the fixed-size context
    // vector column; omitting it lets the provider fall back to its default dimensionality.
    if (config.dimensions > 0) {
        req.AddMember("dimensions", config.dimensions, allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    req.Accept(writer);
    std::string request_body(buffer.GetString(), buffer.GetSize());

    HttpClient client;
    RETURN_IF_ERROR(client.init(config.endpoint));
    client.set_method(POST);
    client.set_content_type("application/json");
    // Only send Authorization when the provider is configured with a key; local providers run
    // without auth and reject/ignore a bogus bearer token.
    if (!config.api_key.empty()) {
        client.set_bearer_token(config.api_key);
    }
    client.set_timeout_ms(config.timeout_ms);

    std::string response;
    RETURN_IF_ERROR(client.execute_post_request(request_body, &response));

    rapidjson::Document doc;
    doc.Parse(response.c_str());
    if (doc.HasParseError() || !doc.IsObject() || !doc.HasMember("data") || !doc["data"].IsArray() ||
        doc["data"].Empty() || !doc["data"][0].IsObject() || !doc["data"][0].HasMember("embedding") ||
        !doc["data"][0]["embedding"].IsArray()) {
        return Status::InternalError("embedding: unexpected response shape from provider");
    }

    std::vector<float> vec;
    const auto& embedding_arr = doc["data"][0]["embedding"];
    vec.reserve(embedding_arr.Size());
    for (auto& v : embedding_arr.GetArray()) {
        // Fail the row rather than silently zeroing a non-numeric component: a 0 in a random
        // dimension corrupts similarity downstream with no signal. The caller turns this error
        // into a NULL embedding for the row.
        if (!v.IsNumber()) {
            return Status::InternalError("embedding: non-numeric element in provider response");
        }
        vec.push_back(static_cast<float>(v.GetDouble()));
    }
    return vec;
}

} // namespace

// embedding(text VARCHAR, config JSON) -> ARRAY<FLOAT>. One synchronous HTTP call per row against
// the OpenAI-compatible embeddings endpoint carried in the config JSON. A null input or a provider
// failure yields a NULL array for that row rather than failing the whole query.
StatusOr<ColumnPtr> EmbeddingFunctions::embedding(FunctionContext* context, const Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("embedding(text VARCHAR, config JSON) takes exactly two arguments");
    }

    const size_t num_rows = columns[0]->size();
    auto text_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[1]);

    // Build a nullable ARRAY<FLOAT>. ArrayColumn requires a Nullable elements column; embedding
    // components are never individually null, so the inner NullColumn stays all-zeros. The outer
    // NullableColumn lets us emit NULL for a whole row on provider failure. ArrayColumn seeds the
    // leading 0 offset in its constructor when the offsets column arrives empty.
    auto elem_ptr = FloatColumn::create();
    auto inner_null_ptr = NullColumn::create();
    auto offsets_ptr = UInt32Column::create();
    FloatColumn* elements_raw = elem_ptr.get();
    NullColumn* element_nulls = inner_null_ptr.get();
    UInt32Column* offsets_raw = offsets_ptr.get();
    auto inner_nullable = NullableColumn::create(std::move(elem_ptr), std::move(inner_null_ptr));
    auto array_col = ArrayColumn::create(std::move(inner_nullable), std::move(offsets_ptr));
    auto null_col = NullColumn::create();
    auto& offsets_data = offsets_raw->get_data();

    // Constant config is the common case (FE passes a single literal config JSON per query); parse
    // it once instead of per row.
    bool config_is_const = context->is_notnull_constant_column(1);
    EmbeddingConfig const_config;
    bool have_const_config = false;
    if (config_is_const && num_rows > 0 && !json_viewer.is_null(0)) {
        JsonValue* json_value = json_viewer.value(0);
        ASSIGN_OR_RETURN(const_config, parse_embedding_config(*json_value));
        have_const_config = true;
    }

    for (size_t row = 0; row < num_rows; ++row) {
        if (text_viewer.is_null(row) || json_viewer.is_null(row)) {
            null_col->append(1);
            offsets_data.push_back(offsets_data.back());
            continue;
        }

        EmbeddingConfig row_config;
        const EmbeddingConfig* config_ptr;
        if (have_const_config) {
            config_ptr = &const_config;
        } else {
            JsonValue* json_value = json_viewer.value(row);
            ASSIGN_OR_RETURN(row_config, parse_embedding_config(*json_value));
            config_ptr = &row_config;
        }

        auto vec_or = call_embedding(text_viewer.value(row).to_string(), *config_ptr);
        if (!vec_or.ok() || vec_or.value().empty()) {
            null_col->append(1);
            offsets_data.push_back(offsets_data.back());
            continue;
        }

        const auto& vec = vec_or.value();
        auto& elem_data = elements_raw->get_data();
        elem_data.insert(elem_data.end(), vec.begin(), vec.end());
        auto& inner_null_data = element_nulls->get_data();
        inner_null_data.insert(inner_null_data.end(), vec.size(), 0);
        offsets_data.push_back(static_cast<uint32_t>(elem_data.size()));
        null_col->append(0);
    }

    return NullableColumn::create(std::move(array_col), std::move(null_col));
}

} // namespace starrocks

#include "gen_cpp/opcode/EmbeddingFunctions.inc"
