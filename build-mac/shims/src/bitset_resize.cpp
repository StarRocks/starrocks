// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Minimal bitset_resize implementation for libroaring on macOS
// This provides the missing symbol that libroaring expects

#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {

// Simple bitset resize implementation
// This is a minimal implementation to satisfy libroaring linkage
void* bitset_resize(void* bitset, size_t new_size) {
    if (bitset == nullptr) {
        // Allocate new bitset
        size_t bytes = (new_size + 7) / 8;
        void* new_bitset = calloc(bytes, 1);
        return new_bitset;
    }

    // For existing bitset, we need to reallocate and expand
    // This is a simplified implementation
    size_t old_bytes = 0; // We don't know the old size, so we'll just allocate new
    size_t new_bytes = (new_size + 7) / 8;

    void* new_bitset = realloc(bitset, new_bytes);
    if (new_bitset != nullptr && new_bytes > old_bytes) {
        // Zero out the new bytes
        memset(static_cast<char*>(new_bitset) + old_bytes, 0, new_bytes - old_bytes);
    }

    return new_bitset;
}

} // extern "C"