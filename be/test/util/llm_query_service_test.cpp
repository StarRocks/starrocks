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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <chrono>
#include <future>
#include <thread>

#include "boost/algorithm/string.hpp"
#include "common/config.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "util/json.h"
#include "util/llm_cache.h"

namespace starrocks {

// Mock LLM API handler for successful responses
class MockLLMHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        // Check for Authorization header
        const auto& auth_header = req->header("Authorization");
        if (auth_header.empty() || !boost::algorithm::starts_with(auth_header, "Bearer ")) {
            HttpChannel::send_error(req, HttpStatus::UNAUTHORIZED);
            return;
        }

        // Check content type for POST requests
        if (req->method() == HttpMethod::POST) {
            const auto& content_type = req->header("Content-Type");
            if (content_type.find("application/json") == std::string::npos) {
                HttpChannel::send_error(req, HttpStatus::BAD_REQUEST);
                return;
            }

            std::string request_body = req->get_request_body();
            if (request_body.empty()) {
                HttpChannel::send_error(req, HttpStatus::BAD_REQUEST);
                return;
            }

            // Parse the request to extract prompt and generate appropriate response
            rapidjson::Document request_doc;
            rapidjson::ParseResult ok = request_doc.Parse(request_body.c_str());
            if (!ok) {
                HttpChannel::send_error(req, HttpStatus::BAD_REQUEST);
                return;
            }

            std::string user_prompt = extractUserPrompt(request_doc);
            std::string ai_response = generateMockResponse(user_prompt);

            // Create OpenAI-style response
            std::string response_body = createOpenAIResponse(ai_response);

            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, response_body);
        } else {
            HttpChannel::send_error(req, HttpStatus::METHOD_NOT_ALLOWED);
        }
    }

private:
    std::string extractUserPrompt(const rapidjson::Document& request_doc) {
        if (request_doc.HasMember("messages") && request_doc["messages"].IsArray()) {
            const auto& messages = request_doc["messages"];
            for (rapidjson::SizeType i = 0; i < messages.Size(); i++) {
                const auto& message = messages[i];
                if (message.HasMember("role") && message["role"].IsString() &&
                    std::string(message["role"].GetString()) == "user" && message.HasMember("content") &&
                    message["content"].IsString()) {
                    return message["content"].GetString();
                }
            }
        }
        return "";
    }

    std::string generateMockResponse(const std::string& prompt) {
        std::string lower_prompt = boost::algorithm::to_lower_copy(prompt);

        if (lower_prompt.find("sentiment") != std::string::npos) {
            if (lower_prompt.find("happy") != std::string::npos || lower_prompt.find("good") != std::string::npos ||
                lower_prompt.find("great") != std::string::npos) {
                return "positive";
            } else if (lower_prompt.find("bad") != std::string::npos ||
                       lower_prompt.find("angry") != std::string::npos ||
                       lower_prompt.find("sad") != std::string::npos ||
                       lower_prompt.find("not happy") != std::string::npos) {
                return "negative";
            }
            return "neutral";
        } else if (lower_prompt.find("summarize") != std::string::npos) {
            return "This is a mock summary of the provided text.";
        } else if (lower_prompt.find("translate") != std::string::npos) {
            return "This is a mock translation.";
        } else if (lower_prompt.find("delay") != std::string::npos) {
            // Simulate slow response for timeout testing
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return "This is a delayed response.";
        } else {
            return "This is a mock AI response for: " + prompt;
        }
    }

    std::string createOpenAIResponse(const std::string& content) {
        // Create response using RapidJSON to ensure proper escaping
        rapidjson::Document response_doc;
        response_doc.SetObject();
        auto& allocator = response_doc.GetAllocator();

        response_doc.AddMember("id", "chatcmpl-mock123", allocator);
        response_doc.AddMember("object", "chat.completion", allocator);
        response_doc.AddMember("created", 1679000000, allocator);
        response_doc.AddMember("model", "gpt-3.5-turbo-mock", allocator);

        // Create choices array
        rapidjson::Value choices(rapidjson::kArrayType);
        rapidjson::Value choice(rapidjson::kObjectType);
        choice.AddMember("index", 0, allocator);

        // Create message object with properly escaped content
        rapidjson::Value message(rapidjson::kObjectType);
        message.AddMember("role", "assistant", allocator);
        message.AddMember("content", rapidjson::Value(content.c_str(), allocator), allocator);
        choice.AddMember("message", message, allocator);
        choice.AddMember("finish_reason", "stop", allocator);

        choices.PushBack(choice, allocator);
        response_doc.AddMember("choices", choices, allocator);

        // Add usage info
        rapidjson::Value usage(rapidjson::kObjectType);
        usage.AddMember("prompt_tokens", 10, allocator);
        usage.AddMember("completion_tokens", 5, allocator);
        usage.AddMember("total_tokens", 15, allocator);
        response_doc.AddMember("usage", usage, allocator);

        // Convert to string
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        response_doc.Accept(writer);
        return buffer.GetString();
    }
};

// Mock handler for error scenarios
class MockLLMErrorHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        const std::string& path = req->uri();

        if (path.find("unauthorized") != std::string::npos) {
            std::string error_response = R"({
                "error": {
                    "message": "Incorrect API key provided",
                    "type": "invalid_request_error",
                    "code": "invalid_api_key"
                }
            })";
            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, error_response);
        } else if (path.find("rate_limit") != std::string::npos) {
            std::string error_response = R"({
                "error": {
                    "message": "Rate limit reached",
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded"
                }
            })";
            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_response);
        } else if (path.find("invalid_model") != std::string::npos) {
            std::string error_response = R"({
                "error": {
                    "message": "The model 'invalid-model' does not exist",
                    "type": "invalid_request_error",
                    "code": "model_not_found"
                }
            })";
            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_response);
        } else if (path.find("timeout") != std::string::npos) {
            // Simulate timeout by sleeping longer than client timeout
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            HttpChannel::send_error(req, HttpStatus::INTERNAL_SERVER_ERROR);
        } else if (path.find("invalid_json") != std::string::npos) {
            // Return invalid JSON response
            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, "invalid json response");
        } else if (path.find("empty_choices") != std::string::npos) {
            // Return valid JSON but with empty choices
            std::string response = R"({
                "id": "chatcmpl-mock123",
                "object": "chat.completion",
                "created": 1679000000,
                "model": "gpt-3.5-turbo-mock",
                "choices": [],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 0,
                    "total_tokens": 10
                }
            })";
            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, response);
        } else if (path.find("missing_message") != std::string::npos) {
            // Return valid JSON but with missing message
            std::string response = R"({
                "id": "chatcmpl-mock123",
                "object": "chat.completion",
                "created": 1679000000,
                "model": "gpt-3.5-turbo-mock",
                "choices": [{
                    "index": 0,
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15
                }
            })";
            req->add_output_header("Content-Type", "application/json");
            HttpChannel::send_reply(req, response);
        } else {
            HttpChannel::send_error(req, HttpStatus::INTERNAL_SERVER_ERROR);
        }
    }
};

// Mock handler for slow responses (for async testing)
class MockSlowLLMHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        // Check authorization
        const auto& auth_header = req->header("Authorization");
        if (auth_header.empty() || !boost::algorithm::starts_with(auth_header, "Bearer ")) {
            HttpChannel::send_error(req, HttpStatus::UNAUTHORIZED);
            return;
        }

        if (req->method() != HttpMethod::POST) {
            HttpChannel::send_error(req, HttpStatus::METHOD_NOT_ALLOWED);
            return;
        }

        // Simulate slow processing
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        std::string response_body = R"({
            "id": "chatcmpl-slow123",
            "object": "chat.completion",
            "created": 1679000000,
            "model": "gpt-3.5-turbo-slow",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "This is a slow response."
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            }
        })";

        req->add_output_header("Content-Type", "application/json");
        HttpChannel::send_reply(req, response_body);
    }
};

static MockLLMHandler s_mock_llm_handler = MockLLMHandler();
static MockLLMErrorHandler s_mock_error_handler = MockLLMErrorHandler();
static MockSlowLLMHandler s_mock_slow_handler = MockSlowLLMHandler();

static EvHttpServer* s_mock_llm_server = nullptr;
static int llm_server_port = 0;
static std::string llm_server_url = "";

class LLMQueryServiceTest : public ::testing::Test {
public:
    LLMQueryServiceTest() = default;
    ~LLMQueryServiceTest() override = default;

    static void SetUpTestCase() {
        // Start mock LLM API server
        s_mock_llm_server = new EvHttpServer(0);

        // Normal endpoints
        s_mock_llm_server->register_handler(POST, "/v1/chat/completions", &s_mock_llm_handler);
        s_mock_llm_server->register_handler(POST, "/chat/completions", &s_mock_llm_handler);

        // Error endpoints
        s_mock_llm_server->register_handler(POST, "/unauthorized", &s_mock_error_handler);
        s_mock_llm_server->register_handler(POST, "/rate_limit", &s_mock_error_handler);
        s_mock_llm_server->register_handler(POST, "/invalid_model", &s_mock_error_handler);
        s_mock_llm_server->register_handler(POST, "/timeout", &s_mock_error_handler);
        s_mock_llm_server->register_handler(POST, "/invalid_json", &s_mock_error_handler);
        s_mock_llm_server->register_handler(POST, "/empty_choices", &s_mock_error_handler);
        s_mock_llm_server->register_handler(POST, "/missing_message", &s_mock_error_handler);

        // Slow endpoint for async testing
        s_mock_llm_server->register_handler(POST, "/slow", &s_mock_slow_handler);

        s_mock_llm_server->start();
        llm_server_port = s_mock_llm_server->get_real_port();
        ASSERT_NE(0, llm_server_port);
        llm_server_url = "http://127.0.0.1:" + std::to_string(llm_server_port);

        // Initialize LLM service
        auto service = LLMQueryService::instance();
        auto init_status = service->init();
        ASSERT_TRUE(init_status.ok()) << "Failed to initialize LLM service: " << init_status.message();
    }

    static void TearDownTestCase() {
        if (s_mock_llm_server) {
            s_mock_llm_server->stop();
            s_mock_llm_server->join();
            delete s_mock_llm_server;
            s_mock_llm_server = nullptr;
        }
    }

    void SetUp() override {}

protected:
    ModelConfig createMockConfig(const std::string& endpoint_suffix = "/v1/chat/completions",
                                 const std::string& model = "gpt-3.5-turbo",
                                 const std::string& api_key = "test-api-key", double temperature = 0.7,
                                 int max_tokens = 1000, double top_p = 0.9) {
        ModelConfig config;
        config.endpoint = llm_server_url + endpoint_suffix;
        config.model = model;
        config.api_key = api_key;
        config.temperature = temperature;
        config.max_tokens = max_tokens;
        config.top_p = top_p;
        return config;
    }
};

// Basic functionality tests
TEST_F(LLMQueryServiceTest, BasicQuery) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    auto result = service->query("Hello, how are you?", config);
    ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().message();
    ASSERT_FALSE(result.value().empty()) << "Response should not be empty";
    ASSERT_EQ(result.value(), "This is a mock AI response for: Hello, how are you?");
}

TEST_F(LLMQueryServiceTest, SentimentAnalysisQuery) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    std::string prompt = "Analyze sentiment: I am very happy today!";
    auto result = service->query(prompt, config);
    ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().message();
    ASSERT_EQ(result.value(), "positive");
}

TEST_F(LLMQueryServiceTest, SummarizeQuery) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    std::string prompt = "Please summarize this long text...";
    auto result = service->query(prompt, config);
    ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().message();
    ASSERT_EQ(result.value(), "This is a mock summary of the provided text.");
}

TEST_F(LLMQueryServiceTest, TranslateQuery) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    std::string prompt = "Please translate this text to English";
    auto result = service->query(prompt, config);
    ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().message();
    ASSERT_EQ(result.value(), "This is a mock translation.");
}

// Async query tests
TEST_F(LLMQueryServiceTest, AsyncQuery) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/slow");

    auto future = service->async_query("Async test query", config);
    ASSERT_TRUE(future.valid()) << "Future should be valid";

    // Wait for result with timeout
    auto status = future.wait_for(std::chrono::seconds(5));
    ASSERT_EQ(status, std::future_status::ready) << "Async query should complete within timeout";

    auto result = future.get();
    ASSERT_TRUE(result.ok()) << "Async query failed: " << result.status().message();
    ASSERT_EQ(result.value(), "This is a slow response.");
}

TEST_F(LLMQueryServiceTest, ConcurrentQueries) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    const int num_queries = 5;
    std::vector<std::shared_future<StatusOr<std::string>>> futures;

    // Submit multiple queries concurrently
    for (int i = 0; i < num_queries; ++i) {
        std::string prompt = "Concurrent query " + std::to_string(i);
        futures.push_back(service->async_query(prompt, config));
    }

    // Wait for all queries to complete
    for (int i = 0; i < num_queries; ++i) {
        auto status = futures[i].wait_for(std::chrono::seconds(5));
        ASSERT_EQ(status, std::future_status::ready) << "Query " << i << " should complete within timeout";

        auto result = futures[i].get();
        ASSERT_TRUE(result.ok()) << "Query " << i << " failed: " << result.status().message();

        std::string expected = "This is a mock AI response for: Concurrent query " + std::to_string(i);
        ASSERT_EQ(result.value(), expected);
    }
}

TEST_F(LLMQueryServiceTest, DuplicateQueryDeduplication) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/slow");

    const std::string prompt = "Duplicate query test";

    // Submit the same query multiple times concurrently
    auto future1 = service->async_query(prompt, config);
    auto future2 = service->async_query(prompt, config);
    auto future3 = service->async_query(prompt, config);

    // All futures should be valid
    ASSERT_TRUE(future1.valid());
    ASSERT_TRUE(future2.valid());
    ASSERT_TRUE(future3.valid());

    // Wait for results
    auto result1 = future1.get();
    auto result2 = future2.get();
    auto result3 = future3.get();

    // All should succeed and return the same result
    ASSERT_TRUE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_TRUE(result3.ok());

    ASSERT_EQ(result1.value(), result2.value());
    ASSERT_EQ(result2.value(), result3.value());
    ASSERT_EQ(result1.value(), "This is a slow response.");
}

// Error handling tests
TEST_F(LLMQueryServiceTest, UnauthorizedError) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/unauthorized", "gpt-3.5-turbo", "invalid-key");

    auto result = service->query("Test query", config);
    ASSERT_FALSE(result.ok()) << "Query should fail with unauthorized error";
    ASSERT_TRUE(result.status().message().find("401") != std::string::npos ||
                result.status().message().find("Unauthorized") != std::string::npos);
}

TEST_F(LLMQueryServiceTest, RateLimitError) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/rate_limit");

    auto result = service->query("Test query", config);
    ASSERT_FALSE(result.ok()) << "Query should fail with rate limit error";
    ASSERT_TRUE(result.status().message().find("400") != std::string::npos ||
                result.status().message().find("rate") != std::string::npos ||
                result.status().message().find("Bad Request") != std::string::npos);
}

TEST_F(LLMQueryServiceTest, InvalidModelError) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/invalid_model", "invalid-model");

    auto result = service->query("Test query", config);
    ASSERT_FALSE(result.ok()) << "Query should fail with invalid model error";
}

TEST_F(LLMQueryServiceTest, InvalidJSONResponse) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/invalid_json");

    auto result = service->query("Test query", config);
    ASSERT_FALSE(result.ok()) << "Query should fail with invalid JSON";
    ASSERT_TRUE(result.status().message().find("non-json") != std::string::npos);
}

TEST_F(LLMQueryServiceTest, EmptyChoicesResponse) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/empty_choices");

    auto result = service->query("Test query", config);
    ASSERT_FALSE(result.ok()) << "Query should fail with empty choices";
    ASSERT_TRUE(result.status().message().find("empty choices") != std::string::npos);
}

TEST_F(LLMQueryServiceTest, MissingMessageResponse) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig("/missing_message");

    auto result = service->query("Test query", config);
    ASSERT_FALSE(result.ok()) << "Query should fail with missing message";
    ASSERT_TRUE(result.status().message().find("message") != std::string::npos);
}

// Configuration tests
TEST_F(LLMQueryServiceTest, DifferentModelConfigurations) {
    auto* service = LLMQueryService::instance();

    // Test different temperature values
    auto config_low_temp = createMockConfig("/v1/chat/completions", "gpt-3.5-turbo", "test-key", 0.1);
    auto config_high_temp = createMockConfig("/v1/chat/completions", "gpt-3.5-turbo", "test-key", 1.8);

    auto result1 = service->query("Test with low temperature", config_low_temp);
    auto result2 = service->query("Test with high temperature", config_high_temp);

    ASSERT_TRUE(result1.ok()) << "Low temperature query failed: " << result1.status().message();
    ASSERT_TRUE(result2.ok()) << "High temperature query failed: " << result2.status().message();

    // Results should be different due to different prompts
    ASSERT_NE(result1.value(), result2.value());
}

TEST_F(LLMQueryServiceTest, DifferentMaxTokensConfiguration) {
    auto* service = LLMQueryService::instance();

    auto config_low_tokens = createMockConfig("/v1/chat/completions", "gpt-3.5-turbo", "test-key", 0.7, 100);
    auto config_high_tokens = createMockConfig("/v1/chat/completions", "gpt-3.5-turbo", "test-key", 0.7, 2000);

    auto result1 = service->query("Test with low max tokens", config_low_tokens);
    auto result2 = service->query("Test with high max tokens", config_high_tokens);

    ASSERT_TRUE(result1.ok()) << "Low max tokens query failed: " << result1.status().message();
    ASSERT_TRUE(result2.ok()) << "High max tokens query failed: " << result2.status().message();
}

// Cache behavior tests (if cache is accessible)
TEST_F(LLMQueryServiceTest, CacheHitBehavior) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    const std::string prompt = "Cache test query";

    // First query - should go to server
    auto start_time1 = std::chrono::steady_clock::now();
    auto result1 = service->query(prompt, config);
    auto end_time1 = std::chrono::steady_clock::now();
    auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time1 - start_time1);

    ASSERT_TRUE(result1.ok()) << "First query failed: " << result1.status().message();

    // Second query with same prompt and config - should hit cache (if implemented)
    auto start_time2 = std::chrono::steady_clock::now();
    auto result2 = service->query(prompt, config);
    auto end_time2 = std::chrono::steady_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time2 - start_time2);

    ASSERT_TRUE(result2.ok()) << "Second query failed: " << result2.status().message();
    ASSERT_EQ(result1.value(), result2.value()) << "Cached result should be identical";
}

// Edge cases and stress tests
TEST_F(LLMQueryServiceTest, EmptyPrompt) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    auto result = service->query("", config);
    ASSERT_TRUE(result.ok()) << "Empty prompt query failed: " << result.status().message();
    ASSERT_EQ(result.value(), "This is a mock AI response for: ");
}

TEST_F(LLMQueryServiceTest, VeryLongPrompt) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    // Create a very long prompt
    std::string long_prompt(10000, 'a');
    long_prompt = "Long prompt test: " + long_prompt;

    auto result = service->query(long_prompt, config);
    ASSERT_TRUE(result.ok()) << "Long prompt query failed: " << result.status().message();
    ASSERT_FALSE(result.value().empty()) << "Response should not be empty for long prompt";
}

TEST_F(LLMQueryServiceTest, SpecialCharactersInPrompt) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    std::string special_prompt = "Special chars: !@#$%^&*(){}[]|\\:;\"'<>,.?/~`±§";
    auto result = service->query(special_prompt, config);
    ASSERT_TRUE(result.ok()) << "Special characters query failed: " << result.status().message();
    ASSERT_FALSE(result.value().empty()) << "Response should not be empty for special characters";
}

TEST_F(LLMQueryServiceTest, UnicodePrompt) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    std::string unicode_prompt = "Unicode test: 你好世界 🌟 Привет мир";
    auto result = service->query(unicode_prompt, config);
    ASSERT_TRUE(result.ok()) << "Unicode query failed: " << result.status().message();
    ASSERT_FALSE(result.value().empty()) << "Response should not be empty for unicode";
}

// Performance tests
TEST_F(LLMQueryServiceTest, ManySequentialQueries) {
    auto* service = LLMQueryService::instance();
    auto config = createMockConfig();

    const int num_queries = 20;
    std::vector<std::string> responses;

    auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < num_queries; ++i) {
        std::string prompt = "Sequential query " + std::to_string(i);
        auto result = service->query(prompt, config);
        ASSERT_TRUE(result.ok()) << "Sequential query " << i << " failed: " << result.status().message();
        responses.push_back(result.value());
    }

    auto end_time = std::chrono::steady_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Verify all responses are unique
    for (int i = 0; i < num_queries; ++i) {
        std::string expected = "This is a mock AI response for: Sequential query " + std::to_string(i);
        ASSERT_EQ(responses[i], expected);
    }

    LOG(INFO) << "Completed " << num_queries << " sequential queries in " << total_duration.count() << "ms";
}

} // namespace starrocks
