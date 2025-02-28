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

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <glog/logging.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include "common/config.h"
#include "fs/fs_s3.h"
#include "runtime/exec_env.h"
#include "runtime/time_types.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/olap_define.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"

namespace starrocks::lake {

static bool _starrocks_format_inited = false;
Aws::SDKOptions aws_sdk_options;

lake::TabletManager* _lake_tablet_manager = nullptr;

void starrocks_format_initialize(void) {
    setenv("STARROCKS_HOME", "./", 0);
    setenv("UDF_RUNTIME_DIR", "./", 0);

    if (!_starrocks_format_inited) {
        fprintf(stderr, "starrocks format module start to initialize\n");
        // load config file
        std::string conffile = std::filesystem::current_path();
        conffile += "/starrocks.conf";
        const char* config_file_path = conffile.c_str();
        std::ifstream ifs(config_file_path);
        if (!ifs.good()) {
            config_file_path = nullptr;
        }
        if (!starrocks::config::init(config_file_path)) {
            LOG(WARNING) << "read config file:" << config_file_path << " failed!";
            return;
        }

        Aws::InitAPI(aws_sdk_options);

        MemInfo::init();
        date::init_date_cache();

        TimezoneUtils::init_time_zones();

        auto ge_init_stat = GlobalEnv::GetInstance()->init();
        CHECK(ge_init_stat.ok()) << "init global env error";

        auto lake_location_provider = std::make_shared<FixedLocationProvider>("");
        _lake_tablet_manager = new lake::TabletManager(lake_location_provider, config::lake_metadata_cache_limit);
        LOG(INFO) << "starrocks format module has been initialized successfully";
        _starrocks_format_inited = true;
    } else {
        LOG(INFO) << "starrocks format module has already been initialized";
    }
}

void starrocks_format_shutdown(void) {
    if (_starrocks_format_inited) {
        LOG(INFO) << "starrocks format module start to deinitialize";
        Aws::ShutdownAPI(aws_sdk_options);
        SAFE_DELETE(_lake_tablet_manager);
        // SAFE_DELETE(_lake_update_manager);
        LOG(INFO) << "starrocks format module has been deinitialized successfully";
    } else {
        LOG(INFO) << "starrocks format module has already been deinitialized";
    }
}

} // namespace starrocks::lake
