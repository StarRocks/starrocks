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

// POSIX shims for macOS build
// Purpose: Provide macOS-compatible implementations for Linux-specific system calls

#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

#ifdef __APPLE__

// fdatasync is not available on macOS, use fsync instead
#ifndef fdatasync
#define fdatasync(fd) fsync(fd)
#endif

// fallocate is not available on macOS, provide a stub implementation
#ifndef fallocate
static inline int fallocate(int fd, int mode, off_t offset, off_t len) {
    // On macOS, we can't pre-allocate space the same way as Linux
    // We'll use ftruncate to extend the file size if needed
    struct stat st;
    if (fstat(fd, &st) == 0 && offset + len > st.st_size) {
        return ftruncate(fd, offset + len);
    }
    return 0;
}
#endif

// O_DIRECT is not available on macOS, define as 0 (no-op)
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

#endif // __APPLE__