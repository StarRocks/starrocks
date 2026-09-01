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

// Entry points of libstarrocks_paimon.so. Compiled only into the shim, never
// into starrocks_be.

#include "connector/hive/paimon/paimon_cpp_shim_api.h"
#include "connector/hive/paimon/paimon_scanner.h"

#ifndef STARROCKS_PAIMON_SHIM_BUILD_VERSION
#error "STARROCKS_PAIMON_SHIM_BUILD_VERSION must be defined by the build system"
#endif

extern "C" starrocks::HdfsScanner* starrocks_paimon_create_scanner() {
    return new starrocks::PaimonScanner();
}

// Must return the compile-time literal, not starrocks::STARROCKS_COMMIT_HASH:
// that extern would resolve against the executable's copy at dlopen time and
// make the check pass trivially.
extern "C" const char* starrocks_paimon_build_version() {
    return STARROCKS_PAIMON_SHIM_BUILD_VERSION;
}
