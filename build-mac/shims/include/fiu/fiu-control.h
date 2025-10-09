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

// Minimal stub of libfiu control header for macOS build
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// External enable API used by StarRocks when integrating failpoints.
// This stub does nothing and returns 0.
static inline int fiu_enable_external(const char* /*name*/, int /*action*/, void* /*failnum*/, unsigned int /*flags*/,
                                      int (*/*callback*/)(const char*, int*, void**, unsigned int*)) {
    return 0;
}

#ifdef __cplusplus
}
#endif

