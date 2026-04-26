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

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __APPLE__

#ifndef fdatasync
#define fdatasync(fd) fsync(fd)
#endif

#ifndef fallocate
static inline int fallocate(int fd, int mode, off_t offset, off_t len) {
    (void)mode;
    struct stat st;
    if (fstat(fd, &st) == 0 && offset + len > st.st_size) {
        return ftruncate(fd, offset + len);
    }
    return 0;
}
#endif

#ifndef O_DIRECT
#define O_DIRECT 0
#endif

#endif
