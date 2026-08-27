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

#include "base/testutil/assert.h"
#include "common/config_lake_fwd.h"
#include "exprs_ext/table_function/list_rowsets.h"
#include "gtest/gtest.h"
#include "platform/platform_env.h"
#include "runtime/runtime_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_env.h"

namespace starrocks {

class ListRowsetsTableFunctionTest : public ::testing::Test {
public:
    void SetUp() override {
        StorageEnv::GetInstance()->stop();
        StorageEnv::GetInstance()->destroy();
        _state = new ListRowsets::MyState();
        _runtimeState = new RuntimeState(static_cast<const QueryExecutionServices*>(nullptr), nullptr);
    }

    void TearDown() override {
        delete _state;
        delete _runtimeState;
        StorageEnvOptions storage_env_options;
        storage_env_options.store_path_registry = PlatformEnv::GetInstance()->store_path_registry();
        storage_env_options.update_mem_tracker = RuntimeEnv::GetInstance()->update_mem_tracker();
        storage_env_options.lake_metadata_cache_limit = config::lake_metadata_cache_limit;
        storage_env_options.lake_location_provider_mode = LakeLocationProviderMode::kFixed;
        ASSERT_OK(StorageEnv::GetInstance()->init(storage_env_options));
    }

protected:
    ListRowsets::MyState* _state;
    RuntimeState* _runtimeState;
};

TEST_F(ListRowsetsTableFunctionTest, basic_test) {
    auto tablet_id_column = Int64Column::create();
    tablet_id_column->append(12345);
    auto tablet_version_column = Int64Column::create();
    tablet_version_column->append(1);

    Columns input_columns;
    input_columns.push_back(tablet_id_column);
    input_columns.push_back(tablet_version_column);

    _state->set_params(input_columns);

    ListRowsets list_rowsets_func;
    auto result = list_rowsets_func.process(_runtimeState, _state);
#ifndef __APPLE__
    ASSERT_EQ("Only works for tablets in the cloud-native table", _state->status().message());
#else
    ASSERT_EQ("Lake storage is disabled on macOS", _state->status().message());
#endif
}
} // namespace starrocks
