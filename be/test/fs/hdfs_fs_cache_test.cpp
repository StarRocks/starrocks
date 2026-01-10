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

#include "fs/hdfs/hdfs_fs_cache.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class HdfsFsCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original config value
        _original_force_new_instance = config::hdfs_client_force_new_instance;
    }

    void TearDown() override {
        // Restore original config value
        config::hdfs_client_force_new_instance = _original_force_new_instance;
    }

    bool _original_force_new_instance;
};

// Test should_force_new_hdfs_instance with nullptr properties
TEST_F(HdfsFsCacheTest, ShouldForceNewInstanceWithNullProperties) {
    // When properties is nullptr, should use global config
    config::hdfs_client_force_new_instance = true;
    EXPECT_TRUE(should_force_new_hdfs_instance(nullptr));

    config::hdfs_client_force_new_instance = false;
    EXPECT_FALSE(should_force_new_hdfs_instance(nullptr));
}

// Test should_force_new_hdfs_instance with properties but disable_cache not set
TEST_F(HdfsFsCacheTest, ShouldForceNewInstanceWithPropertiesNoDisableCache) {
    THdfsProperties properties;
    // disable_cache is not set (__isset.disable_cache = false)

    config::hdfs_client_force_new_instance = true;
    EXPECT_TRUE(should_force_new_hdfs_instance(&properties));

    config::hdfs_client_force_new_instance = false;
    EXPECT_FALSE(should_force_new_hdfs_instance(&properties));
}

// Test should_force_new_hdfs_instance with disable_cache=true overriding global config
TEST_F(HdfsFsCacheTest, ShouldForceNewInstanceDisableCacheTrueOverride) {
    THdfsProperties properties;
    properties.__isset.disable_cache = true;
    properties.disable_cache = true;

    // disable_cache=true should override global config=false
    config::hdfs_client_force_new_instance = false;
    EXPECT_TRUE(should_force_new_hdfs_instance(&properties));

    // disable_cache=true should be consistent with global config=true
    config::hdfs_client_force_new_instance = true;
    EXPECT_TRUE(should_force_new_hdfs_instance(&properties));
}

// Test should_force_new_hdfs_instance with disable_cache=false overriding global config
TEST_F(HdfsFsCacheTest, ShouldForceNewInstanceDisableCacheFalseOverride) {
    THdfsProperties properties;
    properties.__isset.disable_cache = true;
    properties.disable_cache = false;

    // disable_cache=false should override global config=true
    config::hdfs_client_force_new_instance = true;
    EXPECT_FALSE(should_force_new_hdfs_instance(&properties));

    // disable_cache=false should be consistent with global config=false
    config::hdfs_client_force_new_instance = false;
    EXPECT_FALSE(should_force_new_hdfs_instance(&properties));
}

// Test HdfsFsClient ownership flag default value
TEST_F(HdfsFsCacheTest, HdfsFsClientOwnershipDefault) {
    HdfsFsClient client;
    // Default should be true (owns the hdfs_fs)
    EXPECT_TRUE(client._owns_hdfs_fs);
}

// Test HdfsFsClient ownership flag can be set
TEST_F(HdfsFsCacheTest, HdfsFsClientOwnershipCanBeSet) {
    HdfsFsClient client;
    client._owns_hdfs_fs = false;
    EXPECT_FALSE(client._owns_hdfs_fs);

    client._owns_hdfs_fs = true;
    EXPECT_TRUE(client._owns_hdfs_fs);
}

// Test HdfsFsClient destructor behavior with _owns_hdfs_fs=false
// When _owns_hdfs_fs is false, hdfsDisconnect should NOT be called
TEST_F(HdfsFsCacheTest, HdfsFsClientDestructorNotOwned) {
    // This test verifies that when _owns_hdfs_fs=false and hdfs_fs=nullptr,
    // the destructor does not crash (no-op)
    {
        HdfsFsClient client;
        client.hdfs_fs = nullptr;
        client._owns_hdfs_fs = false;
        // Destructor will be called at end of scope
    }
    // If we get here without crash, test passes
    SUCCEED();
}

// Test HdfsFsClient destructor behavior with _owns_hdfs_fs=true and nullptr
TEST_F(HdfsFsCacheTest, HdfsFsClientDestructorOwnedButNull) {
    // This test verifies that when _owns_hdfs_fs=true but hdfs_fs=nullptr,
    // the destructor does not crash (condition checks nullptr first)
    {
        HdfsFsClient client;
        client.hdfs_fs = nullptr;
        client._owns_hdfs_fs = true;
        // Destructor will be called at end of scope
    }
    // If we get here without crash, test passes
    SUCCEED();
}

} // namespace starrocks

