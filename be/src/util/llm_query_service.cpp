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

#include "util/llm_query_service.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <memory>

#include "common/config.h"
#include "exprs/ai_functions.h"
#include "http/http_client.h"
#include "util/json.h"
#include "util/llm_cache.h"
#include "util/threadpool.h"

namespace starrocks {

LLMQueryService* LLMQueryService::instance() {
    static LLMQueryService service;
    return &service;
}

LLMQueryService::LLMQueryService() = default;

LLMQueryService::~LLMQueryService() {
    if (_thread_pool) {
        _thread_pool->shutdown();
    }
}

Status LLMQueryService::init_llm_cache() {
    static std::once_flag init_flag;
    std::call_once(init_flag, [this]() { this->_llm_cache.init(config::llm_cache_size); });
    return Status::OK();
}

Status LLMQueryService::init() {
    // Initialize thread pool with configurable number of threads
    int max_concurrent_queries = config::llm_max_concurrent_queries;
    if (max_concurrent_queries <= 0) {
        max_concurrent_queries = std::thread::hardware_concurrency();
    }
    int max_queue_size = config::llm_max_queue_size;

    ThreadPoolBuilder builder("llm_query_pool");
    builder.set_min_threads(1).set_max_threads(max_concurrent_queries).set_max_queue_size(max_queue_size);

    RETURN_IF_ERROR(builder.build(&_thread_pool));

    // Initialize llm_cache
    RETURN_IF_ERROR(init_llm_cache());
    return Status::OK();
}

StatusOr<std::string> LLMQueryService::query(const std::string& prompt, const ModelConfig& config) {
    auto future = async_query(prompt, config);
    return future.get();
}

std::shared_future<StatusOr<std::string>> LLMQueryService::async_query(const std::string& prompt,
                                                                       const ModelConfig& config) {
    std::string cache_key = generate_cache_key(prompt, config);

    // First check cache
    auto cache_value = _llm_cache.lookup(CacheKey(cache_key));
    if (cache_value) {
        std::promise<StatusOr<std::string>> promise;
        promise.set_value(cache_value.value());
        return promise.get_future();
    }

    // Check if same query
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _pending_queries.find(cache_key);
        if (it != _pending_queries.end()) {
            return it->second;
        }
    }

    // Create new query task
    auto task = [this, prompt, config, cache_key]() -> StatusOr<std::string> {
        auto result = execute_query(prompt, config);

        // Cache the result if successful
        if (result.ok()) {
            _llm_cache.insert(cache_key, result.value());
        }

        // Clean up pending query
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _pending_queries.erase(cache_key);
        }

        return result;
    };

    // Submit to thread pool
    auto promise_ptr = std::make_shared<std::promise<StatusOr<std::string>>>();
    auto future = promise_ptr->get_future().share();

    {
        std::lock_guard<std::mutex> lock(_mutex);
        // Check again in case another thread added it while we were waiting
        auto it = _pending_queries.find(cache_key);
        if (it != _pending_queries.end()) {
            return it->second;
        }

        _pending_queries.emplace(cache_key, future);
    }

    // Submit the task to thread pool
    auto status = _thread_pool->submit_func(
            [task = std::move(task), promise_ptr]() mutable { promise_ptr->set_value(task()); });
    if (!status.ok()) {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _pending_queries.erase(cache_key);
        }
        std::promise<StatusOr<std::string>> error_promise;
        error_promise.set_value(
                Status::InternalError(strings::Substitute("Submit to thread pool failed, $0", status.to_string())));
        return error_promise.get_future();
    }

    return future;
}

StatusOr<std::string> LLMQueryService::execute_query(const std::string& prompt, const ModelConfig& config) {
    rapidjson::Document request_doc;
    request_doc.SetObject();
    auto& allocator = request_doc.GetAllocator();

    // Set model
    request_doc.AddMember("model", rapidjson::Value(config.model.c_str(), allocator), allocator);

    // Set temperature, max_tokens, top_p
    request_doc.AddMember("temperature", config.temperature, allocator);
    request_doc.AddMember("max_tokens", config.max_tokens, allocator);
    request_doc.AddMember("top_p", config.top_p, allocator);

    // Build messages array
    rapidjson::Value messages(rapidjson::kArrayType);
    rapidjson::Value user_message(rapidjson::kObjectType);
    user_message.AddMember("role", "user", allocator);
    user_message.AddMember("content", rapidjson::Value(prompt.c_str(), allocator), allocator);
    messages.PushBack(user_message, allocator);
    request_doc.AddMember("messages", messages, allocator);

    // Convert to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    request_doc.Accept(writer);
    std::string request_body = buffer.GetString();

    HttpClient client;
    RETURN_IF_ERROR(client.init(config.endpoint));
    client.set_method(POST);
    client.set_content_type("application/json");
    client.set_bearer_token(config.api_key);
    client.set_timeout_ms(config.timeout_ms);

    std::string response;

    RETURN_IF_ERROR(client.execute_post_request(request_body, &response));

    rapidjson::Document doc;
    rapidjson::ParseResult ok = doc.Parse(response.c_str());

    if (!ok) {
        return Status::RuntimeError("Large language model return a non-json document");
    }

    if (!doc.HasMember("choices") || !doc["choices"].IsArray() || doc["choices"].Empty()) {
        return Status::RuntimeError("Invalid response: missing or empty choices array");
    }

    const auto& first_choice = doc["choices"][0];
    if (!first_choice.HasMember("message") || !first_choice["message"].IsObject()) {
        return Status::RuntimeError("Invalid response: missing message object");
    }

    const auto& message = first_choice["message"];
    if (!message.HasMember("content") || !message["content"].IsString()) {
        return Status::RuntimeError("Invalid response: missing or invalid content field");
    }

    return message["content"].GetString();
}

} // namespace starrocks
