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

#pragma once

// Platform-specific header includes for compression libraries
// On macOS (Homebrew), headers are installed without subdirectories
// On Linux (thirdparty), headers are in subdirectories

#ifdef STARROCKS_MACOS_USE_FLAT_INCLUDES
// macOS: Homebrew installs headers as /opt/homebrew/opt/lz4/include/lz4.h
#include <lz4.h>
#include <lz4frame.h>
#include <lz4hc.h>
#include <zstd.h>
#include <zstd_errors.h>
#else
// Linux: thirdparty installs headers as include/lz4/lz4.h
#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <lz4/lz4hc.h>
#include <zstd/zstd.h>
#include <zstd/zstd_errors.h>
#endif
