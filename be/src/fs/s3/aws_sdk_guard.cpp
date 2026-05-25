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

#ifndef __APPLE__
#include "fs/s3/aws_sdk_guard.h"

#include <aws/core/utils/logging/LogLevel.h>

#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>

#include "common/config_object_storage_fwd.h"
#include "common/configbase.h"
#include "fs/s3/poco_http_client_factory.h"

namespace starrocks {

namespace {

Aws::Utils::Logging::LogLevel parse_aws_sdk_log_level(const std::string& s) {
    Aws::Utils::Logging::LogLevel levels[] = {
            Aws::Utils::Logging::LogLevel::Off,   Aws::Utils::Logging::LogLevel::Fatal,
            Aws::Utils::Logging::LogLevel::Error, Aws::Utils::Logging::LogLevel::Warn,
            Aws::Utils::Logging::LogLevel::Info,  Aws::Utils::Logging::LogLevel::Debug,
            Aws::Utils::Logging::LogLevel::Trace,
    };
    std::string slevel = boost::algorithm::to_upper_copy(s);
    Aws::Utils::Logging::LogLevel level = Aws::Utils::Logging::LogLevel::Warn;
    for (auto& idx : levels) {
        auto name = Aws::Utils::Logging::GetLogLevelName(idx);
        if (name == slevel) {
            level = idx;
            break;
        }
    }
    return level;
}

} // namespace

AwsSdkGuard::AwsSdkGuard() {
    // libcurl is already initialized beforehand.
    _options.httpOptions.initAndCleanupCurl = false;
    if (config::aws_sdk_logging_trace_enabled) {
        auto level = parse_aws_sdk_log_level(config::aws_sdk_logging_trace_level);
        std::cerr << "enable aws sdk logging trace. log level = " << Aws::Utils::Logging::GetLogLevelName(level)
                  << "\n";
        _options.loggingOptions.logLevel = level;
    }
    if (config::aws_sdk_enable_compliant_rfc3986_encoding) {
        _options.httpOptions.compliantRfc3986Encoding = true;
    }
    Aws::InitAPI(_options);
    if (config::enable_poco_client_for_aws_sdk) {
        Aws::Http::SetHttpClientFactory(std::make_shared<poco::PocoHttpClientFactory>());
    }
}

AwsSdkGuard::~AwsSdkGuard() {
    Aws::ShutdownAPI(_options);
}

} // namespace starrocks
#endif
