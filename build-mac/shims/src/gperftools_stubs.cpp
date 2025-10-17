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

// Stub implementations for gperftools/TCMalloc functions on macOS
// We use jemalloc instead of TCMalloc on macOS

extern "C" {

// MallocExtension function (TCMalloc-specific)
// This is called periodically by brpc to release free memory
void MallocExtension_ReleaseFreeMemory() {
    // On jemalloc, we don't need to do anything special
    // jemalloc manages memory automatically
}

// CPU profiling functions (gperftools-specific)
// CPU profiling is not supported on macOS build
int ProfilerStart(const char* fname) {
    // CPU profiling not supported on macOS build
    return 0;
}

void ProfilerStop() {
    // CPU profiling not supported on macOS build
}

} // extern "C"
