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

#include <gtest/gtest.h>

#include <cstdio>

#include "common/configbase.h"
#include "formats/orc/lzo_decompressor_registration.h"
#include "types/time_types.h"

int main(int argc, char** argv) {
    auto lzo_status = starrocks::register_orc_lzo_decompressor();
    if (!lzo_status.ok()) {
        std::fprintf(stderr, "fail to register ORC LZO decompressor: %s\n", lzo_status.to_string().c_str());
        return 1;
    }

    if (!starrocks::config::init(nullptr)) {
        std::fprintf(stderr, "failed to initialize config defaults\n");
        return 1;
    }
    starrocks::date::init_date_cache();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
