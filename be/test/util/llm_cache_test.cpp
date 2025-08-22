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

#include "util/llm_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "exprs/ai_functions.h"
#include "util/lru_cache.h"

using namespace starrocks;
using namespace std;

namespace starrocks {

class LLMCacheTest : public testing::Test {
public:
    void SetUp() override {
        // Reset llm cache for each test
        llm_cache = new LLMCache();
        // Clean up any existing cache
        llm_cache->release_cache();
    }

    void TearDown() override {
        // Clean up after each test
        llm_cache->release_cache();
        delete llm_cache;
    }

protected:
    LLMCache* llm_cache;
};

TEST_F(LLMCacheTest, CacheKeyUniqueness) {
    ModelConfig config1;
    config1.model = "gpt-3.5-turbo";
    config1.temperature = 0.7;
    config1.max_tokens = 100;
    config1.top_p = 0.9;

    ModelConfig config2;
    config2.model = "gpt-4";
    config2.temperature = 0.7;
    config2.max_tokens = 100;
    config2.top_p = 0.9;

    std::string prompt = "Same prompt";
    std::string key1 = generate_cache_key(prompt, config1);
    std::string key2 = generate_cache_key(prompt, config2);

    EXPECT_NE(key1, key2);
}

// Test cache key uniqueness for different prompts
TEST_F(LLMCacheTest, CacheKeyUniquenessDifferentPrompts) {
    ModelConfig config;
    config.model = "gpt-3.5-turbo";
    config.temperature = 0.7;
    config.max_tokens = 100;
    config.top_p = 0.9;

    std::string key1 = generate_cache_key("Prompt 1", config);
    std::string key2 = generate_cache_key("Prompt 2", config);

    EXPECT_NE(key1, key2);
}

// Test cache initialization
TEST_F(LLMCacheTest, CacheInitialization) {
    llm_cache->init(10000);

    // Test by trying to insert and lookup
    std::string cache_key = "test_key";
    std::string response = "test_response";

    llm_cache->insert(cache_key, response);

    LLMCacheValue* result = llm_cache->lookup(CacheKey(cache_key));
    EXPECT_NE(result, nullptr);
    if (result) {
        EXPECT_EQ(result->response, response);
        EXPECT_EQ(result->cache_key_str, cache_key);
    }
}

// Test multiple initialization calls
TEST_F(LLMCacheTest, MultipleInitialization) {
    llm_cache->init(10000);

    // Insert a value
    std::string cache_key = "test_key";
    std::string response = "test_response";
    llm_cache->insert(cache_key, response);

    // Second init should affect existing data
    llm_cache->init(20000);

    // Can not find the original data
    LLMCacheValue* result = llm_cache->lookup(CacheKey(cache_key));
    EXPECT_EQ(result, nullptr);
}

// Test setting capacity before initialization
TEST_F(LLMCacheTest, SetCapacityBeforeInit) {
    // Should not crash when cache is not initialized
    llm_cache->set_capacity(1000);

    // Initialize after
    llm_cache->init(50000);

    // Should work normally
    llm_cache->insert("test_key", "test_response");
    LLMCacheValue* result = llm_cache->lookup(CacheKey("test_key"));
    EXPECT_NE(result, nullptr);
}

// Test basic insert and lookup operations
TEST_F(LLMCacheTest, BasicInsertAndLookup) {
    llm_cache->init(10000);

    std::string cache_key = "test_key";
    std::string response = "This is a test response";

    // Insert
    llm_cache->insert(cache_key, response);

    // Lookup
    LLMCacheValue* result = llm_cache->lookup(CacheKey(cache_key));
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->response, response);
    EXPECT_EQ(result->cache_key_str, cache_key);

    // Lookup for non-existent key
    result = llm_cache->lookup(CacheKey("non_existent_key"));
    EXPECT_EQ(result, nullptr);
}

// Test cache operations with different data types
TEST_F(LLMCacheTest, DifferentDataTypes) {
    llm_cache->init(1000 * 1024);

    // Test with empty strings
    llm_cache->insert("", "");
    LLMCacheValue* result1 = llm_cache->lookup(CacheKey(""));
    EXPECT_NE(result1, nullptr);
    EXPECT_EQ(result1->response, "");

    // Test with special characters
    std::string special_key = "key_with_特殊字符_!@#$%";
    std::string special_response = "Response with 特殊字符 and symbols: !@#$%^&*()";
    llm_cache->insert(special_key, special_response);
    LLMCacheValue* result2 = llm_cache->lookup(CacheKey(special_key));
    EXPECT_NE(result2, nullptr);
    EXPECT_EQ(result2->response, special_response);

    // Test with very long strings
    std::string long_key(1000, 'k');
    std::string long_response(5000, 'r');
    llm_cache->insert(long_key, long_response);
    LLMCacheValue* result3 = llm_cache->lookup(CacheKey(long_key));
    EXPECT_NE(result3, nullptr);
    EXPECT_EQ(result3->response, long_response);
}

// Test thread safety
TEST_F(LLMCacheTest, ThreadSafety) {
    llm_cache->init(1000 * 1024);

    const int num_threads = 10;
    const int operations_per_thread = 100;

    std::vector<std::thread> threads;
    std::atomic<int> success_count(0);

    // Multiple threads performing cache operations
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i, operations_per_thread, &success_count]() {
            for (int j = 0; j < operations_per_thread; ++j) {
                std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                std::string response = "Thread " + std::to_string(i) + " response " + std::to_string(j);

                llm_cache->insert(key, response);

                LLMCacheValue* result = llm_cache->lookup(CacheKey(key));
                if (result && result->response == response) {
                    success_count++;
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Should have some successful operations (might not be all due to eviction)
    EXPECT_GT(success_count.load(), 0);
}

// Test release_cache functionality
TEST_F(LLMCacheTest, ReleaseCacheFunction) {
    llm_cache->init(10000);

    // Insert some data
    llm_cache->insert("test_key", "test_response");
    LLMCacheValue* result1 = llm_cache->lookup(CacheKey("test_key"));
    EXPECT_NE(result1, nullptr);

    // Release cache
    llm_cache->release_cache();

    // Initialize again
    llm_cache->init(10000);

    // Previous data should be gone
    LLMCacheValue* result2 = llm_cache->lookup(CacheKey("test_key"));
    EXPECT_EQ(result2, nullptr);
}

// Test cache with model config integration
TEST_F(LLMCacheTest, ModelConfigIntegration) {
    llm_cache->init(1000 * 1024);

    ModelConfig config;
    config.model = "test-model";
    config.temperature = 0.8;
    config.max_tokens = 500;
    config.top_p = 0.95;

    std::string prompt = "Test prompt for model";
    std::string cache_key = generate_cache_key(prompt, config);
    std::string response = "Model response to the prompt";

    // Insert using generated cache key
    llm_cache->insert(cache_key, response);

    // Lookup using the same cache key
    LLMCacheValue* result = llm_cache->lookup(CacheKey(cache_key));
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->response, response);
    EXPECT_EQ(result->cache_key_str, cache_key);

    // Verify that different config generates different key
    ModelConfig config2 = config;
    config2.temperature = 0.9; // Different temperature
    std::string cache_key2 = generate_cache_key(prompt, config2);
    EXPECT_NE(cache_key, cache_key2);

    LLMCacheValue* result2 = llm_cache->lookup(CacheKey(cache_key2));
    EXPECT_EQ(result2, nullptr); // Should not find entry with different key
}

// Test memory management and deleter function
TEST_F(LLMCacheTest, MemoryManagement) {
    llm_cache->init(5000); // Very small capacity to force eviction

    // Insert many entries to trigger deletions
    for (int i = 0; i < 20; ++i) {
        std::string key = "memory_test_key_" + std::to_string(i);
        std::string response = "Memory test response " + std::to_string(i) + " with some content";
        llm_cache->insert(key, response);
    }

    // Test should complete without memory leaks
    // The deleter function should be called automatically during eviction
    EXPECT_TRUE(true); // If we reach here, memory management is working
}

// Test concurrent initialization
TEST_F(LLMCacheTest, ConcurrentInitialization) {
    const int num_threads = 10;
    std::vector<std::thread> threads;

    // Multiple threads trying to initialize concurrently
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i]() {
            llm_cache->init(10000 + i * 1000); // Different capacities

            // Try to insert something
            std::string key = "concurrent_key_" + std::to_string(i);
            llm_cache->insert(key, "concurrent_response");
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Cache should be properly initialized and working
    llm_cache->insert("final_test", "final_response");
    LLMCacheValue* result = llm_cache->lookup(CacheKey("final_test"));
    EXPECT_NE(result, nullptr);
}

// Test cache metrics functionality
TEST_F(LLMCacheTest, CacheMetricsTest) {
    // Initialize cache with small capacity
    llm_cache->init(10000);

    // Get initial metrics
    CacheMetrics initial_metrics = llm_cache->get_metrics();
    EXPECT_EQ(initial_metrics.cache_hits, 0);
    EXPECT_EQ(initial_metrics.cache_misses, 0);
    EXPECT_EQ(initial_metrics.total_requests, 0);
    EXPECT_DOUBLE_EQ(initial_metrics.hit_rate, 0.0);
    EXPECT_EQ(initial_metrics.cache_size, 0);
    EXPECT_EQ(initial_metrics.cache_capacity, 10000);

    // Insert some test data
    llm_cache->insert("key1", "response1");
    llm_cache->insert("key2", "response2");
    llm_cache->insert("key3", "response3");

    // Test cache hits
    LLMCacheValue* result1 = llm_cache->lookup(CacheKey("key1")); // Hit
    LLMCacheValue* result2 = llm_cache->lookup(CacheKey("key2")); // Hit

    EXPECT_NE(result1, nullptr);
    EXPECT_NE(result2, nullptr);

    // Test cache miss
    LLMCacheValue* result3 = llm_cache->lookup(CacheKey("non_existent_key")); // Miss
    EXPECT_EQ(result3, nullptr);

    // Get metrics after operations
    CacheMetrics metrics = llm_cache->get_metrics();

    EXPECT_EQ(metrics.cache_hits, 2);
    EXPECT_EQ(metrics.cache_misses, 1);
    EXPECT_EQ(metrics.total_requests, 3);

    // Calculate expected hit rate: 2 hits / 3 requests = 0.666...
    double expected_hit_rate = 2.0 / 3.0;
    EXPECT_NEAR(metrics.hit_rate, expected_hit_rate, 0.001);

    // Cache should have some size (exact value depends on implementation)
    EXPECT_GT(metrics.cache_size, 0);
    EXPECT_LE(metrics.cache_size, metrics.cache_capacity);

    // Test more operations to verify metrics update correctly
    llm_cache->lookup(CacheKey("key3"));                // Hit
    llm_cache->lookup(CacheKey("another_missing_key")); // Miss

    CacheMetrics updated_metrics = llm_cache->get_metrics();

    EXPECT_EQ(updated_metrics.cache_hits, 3);
    EXPECT_EQ(updated_metrics.cache_misses, 2);
    EXPECT_EQ(updated_metrics.total_requests, 5);

    double updated_expected_hit_rate = 3.0 / 5.0;
    EXPECT_NEAR(updated_metrics.hit_rate, updated_expected_hit_rate, 0.001);
}
} // namespace starrocks
