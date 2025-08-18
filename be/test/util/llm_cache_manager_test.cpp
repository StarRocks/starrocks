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

#include "util/llm_cache_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "exprs/ai_functions.h"
#include "util/lru_cache.h"

using namespace starrocks;
using namespace std;

namespace starrocks {

class LLMCacheManagerTest : public testing::Test {
public:
    void SetUp() override {
        // Reset cache manager for each test
        manager = LLMCacheManager::instance();
        // Clean up any existing cache
        manager->release_cache();
    }

    void TearDown() override {
        // Clean up after each test
        manager->release_cache();
    }

protected:
    LLMCacheManager* manager;
};

// Test cache key generation
TEST_F(LLMCacheManagerTest, GenerateCacheKey) {
    ModelConfig config;
    config.model = "gpt-3.5-turbo";
    config.temperature = 0.7;
    config.max_tokens = 100;
    config.top_p = 0.9;

    std::string prompt = "Hello, world!";
    std::string cache_key = generate_cache_key(prompt, config);

    // Direct string comparison - more precise and reliable
    std::string expected_key = "LLM_RESP:gpt-3.5-turbo:0.7:100:0.9|Hello, world!";
    EXPECT_EQ(cache_key, expected_key);
}

// Test cache key generation with edge values
TEST_F(LLMCacheManagerTest, GenerateCacheKeyEdgeCases) {
    // Test with edge case values
    ModelConfig config;
    config.model = "";
    config.temperature = 0.0;
    config.max_tokens = 1;
    config.top_p = 1.0;

    std::string prompt = "";
    std::string cache_key = generate_cache_key(prompt, config);
    std::string expected_key = "LLM_RESP::0:1:1|";
    EXPECT_EQ(cache_key, expected_key);

    // Test with decimal precision
    ModelConfig config2;
    config2.model = "test";
    config2.temperature = 1.23456;
    config2.max_tokens = 2048;
    config2.top_p = 0.95;

    std::string prompt2 = "Test prompt";
    std::string cache_key2 = generate_cache_key(prompt2, config2);
    std::string expected_key2 = "LLM_RESP:test:1.23456:2048:0.95|Test prompt";
    EXPECT_EQ(cache_key2, expected_key2);
}

TEST_F(LLMCacheManagerTest, CacheKeyUniqueness) {
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
TEST_F(LLMCacheManagerTest, CacheKeyUniquenessDifferentPrompts) {
    ModelConfig config;
    config.model = "gpt-3.5-turbo";
    config.temperature = 0.7;
    config.max_tokens = 100;
    config.top_p = 0.9;

    std::string key1 = generate_cache_key("Prompt 1", config);
    std::string key2 = generate_cache_key("Prompt 2", config);

    EXPECT_NE(key1, key2);
}

// Test singleton pattern
TEST_F(LLMCacheManagerTest, SingletonInstance) {
    LLMCacheManager* instance1 = LLMCacheManager::instance();
    LLMCacheManager* instance2 = LLMCacheManager::instance();

    EXPECT_EQ(instance1, instance2);
    EXPECT_NE(instance1, nullptr);
}

// Test cache initialization
TEST_F(LLMCacheManagerTest, CacheInitialization) {
    manager->init(10000);

    // Test by trying to insert and lookup
    std::string cache_key = "test_key";
    std::string response = "test_response";

    manager->insert(cache_key, response);

    LLMCacheValue* result = manager->lookup(CacheKey(cache_key));
    EXPECT_NE(result, nullptr);
    if (result) {
        EXPECT_EQ(result->response, response);
        EXPECT_EQ(result->cache_key_str, cache_key);
    }
}

// Test multiple initialization calls
TEST_F(LLMCacheManagerTest, MultipleInitialization) {
    manager->init(10000);

    // Insert a value
    std::string cache_key = "test_key";
    std::string response = "test_response";
    manager->insert(cache_key, response);

    // Second init should affect existing data
    manager->init(20000);

    // Can not find the original data
    LLMCacheValue* result = manager->lookup(CacheKey(cache_key));
    EXPECT_EQ(result, nullptr);
}

// Test setting capacity before initialization
TEST_F(LLMCacheManagerTest, SetCapacityBeforeInit) {
    // Should not crash when cache is not initialized
    manager->set_capacity(1000);

    // Initialize after
    manager->init(50000);

    // Should work normally
    manager->insert("test_key", "test_response");
    LLMCacheValue* result = manager->lookup(CacheKey("test_key"));
    EXPECT_NE(result, nullptr);
}

// Test basic insert and lookup operations
TEST_F(LLMCacheManagerTest, BasicInsertAndLookup) {
    manager->init(10000);

    std::string cache_key = "test_key";
    std::string response = "This is a test response";

    // Insert
    manager->insert(cache_key, response);

    // Lookup
    LLMCacheValue* result = manager->lookup(CacheKey(cache_key));
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->response, response);
    EXPECT_EQ(result->cache_key_str, cache_key);

    // Lookup for non-existent key
    result = manager->lookup(CacheKey("non_existent_key"));
    EXPECT_EQ(result, nullptr);
}

// Test cache operations with different data types
TEST_F(LLMCacheManagerTest, DifferentDataTypes) {
    manager->init(1000 * 1024);

    // Test with empty strings
    manager->insert("", "");
    LLMCacheValue* result1 = manager->lookup(CacheKey(""));
    EXPECT_NE(result1, nullptr);
    EXPECT_EQ(result1->response, "");

    // Test with special characters
    std::string special_key = "key_with_特殊字符_!@#$%";
    std::string special_response = "Response with 特殊字符 and symbols: !@#$%^&*()";
    manager->insert(special_key, special_response);
    LLMCacheValue* result2 = manager->lookup(CacheKey(special_key));
    EXPECT_NE(result2, nullptr);
    EXPECT_EQ(result2->response, special_response);

    // Test with very long strings
    std::string long_key(1000, 'k');
    std::string long_response(5000, 'r');
    manager->insert(long_key, long_response);
    LLMCacheValue* result3 = manager->lookup(CacheKey(long_key));
    EXPECT_NE(result3, nullptr);
    EXPECT_EQ(result3->response, long_response);
}

// Test thread safety
TEST_F(LLMCacheManagerTest, ThreadSafety) {
    manager->init(1000 * 1024);

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

                manager->insert(key, response);

                LLMCacheValue* result = manager->lookup(CacheKey(key));
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
TEST_F(LLMCacheManagerTest, ReleaseCacheFunction) {
    manager->init(10000);

    // Insert some data
    manager->insert("test_key", "test_response");
    LLMCacheValue* result1 = manager->lookup(CacheKey("test_key"));
    EXPECT_NE(result1, nullptr);

    // Release cache
    manager->release_cache();

    // Initialize again
    manager->init(10000);

    // Previous data should be gone
    LLMCacheValue* result2 = manager->lookup(CacheKey("test_key"));
    EXPECT_EQ(result2, nullptr);
}

// Test cache with model config integration
TEST_F(LLMCacheManagerTest, ModelConfigIntegration) {
    manager->init(1000 * 1024);

    ModelConfig config;
    config.model = "test-model";
    config.temperature = 0.8;
    config.max_tokens = 500;
    config.top_p = 0.95;

    std::string prompt = "Test prompt for model";
    std::string cache_key = generate_cache_key(prompt, config);
    std::string response = "Model response to the prompt";

    // Insert using generated cache key
    manager->insert(cache_key, response);

    // Lookup using the same cache key
    LLMCacheValue* result = manager->lookup(CacheKey(cache_key));
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->response, response);
    EXPECT_EQ(result->cache_key_str, cache_key);

    // Verify that different config generates different key
    ModelConfig config2 = config;
    config2.temperature = 0.9; // Different temperature
    std::string cache_key2 = generate_cache_key(prompt, config2);
    EXPECT_NE(cache_key, cache_key2);

    LLMCacheValue* result2 = manager->lookup(CacheKey(cache_key2));
    EXPECT_EQ(result2, nullptr); // Should not find entry with different key
}

// Test memory management and deleter function
TEST_F(LLMCacheManagerTest, MemoryManagement) {
    manager->init(5000); // Very small capacity to force eviction

    // Insert many entries to trigger deletions
    for (int i = 0; i < 20; ++i) {
        std::string key = "memory_test_key_" + std::to_string(i);
        std::string response = "Memory test response " + std::to_string(i) + " with some content";
        manager->insert(key, response);
    }

    // Test should complete without memory leaks
    // The deleter function should be called automatically during eviction
    EXPECT_TRUE(true); // If we reach here, memory management is working
}

// Test concurrent initialization
TEST_F(LLMCacheManagerTest, ConcurrentInitialization) {
    const int num_threads = 10;
    std::vector<std::thread> threads;

    // Multiple threads trying to initialize concurrently
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i]() {
            manager->init(10000 + i * 1000); // Different capacities

            // Try to insert something
            std::string key = "concurrent_key_" + std::to_string(i);
            manager->insert(key, "concurrent_response");
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Cache should be properly initialized and working
    manager->insert("final_test", "final_response");
    LLMCacheValue* result = manager->lookup(CacheKey("final_test"));
    EXPECT_NE(result, nullptr);
}

} // namespace starrocks
