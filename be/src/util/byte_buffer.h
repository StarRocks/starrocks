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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/byte_buffer.h

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

#pragma once

#include <cstddef>
#include <cstring>
#include <memory>

#include "common/logging.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/utils.h"
#include "testutil/sync_point.h"

namespace starrocks {

struct ByteBuffer;
using ByteBufferPtr = std::shared_ptr<ByteBuffer>;

struct MemTrackerDeleter {
    MemTrackerDeleter(MemTracker* tracker_) : tracker(tracker_) { DCHECK(tracker_ != nullptr); }
    MemTracker* tracker;
    template <typename T>
    void operator()(T* ptr) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
        if (ptr) {
            delete ptr;
        }
    }
};

struct ByteBuffer {
    static StatusOr<ByteBufferPtr> allocate_with_tracker(size_t size) {
        auto tracker = CurrentThread::mem_tracker();
        if (tracker == nullptr) {
            return Status::InternalError("current thread memory tracker Not Found when allocate ByteBuffer");
        }
#ifndef BE_TEST
        // check limit before allocation
        TRY_CATCH_BAD_ALLOC(ByteBufferPtr ptr(new ByteBuffer(size), MemTrackerDeleter(tracker)); return ptr;);
#else
        ByteBufferPtr ptr(new ByteBuffer(size), MemTrackerDeleter(tracker));
        Status ret = Status::OK();
        TEST_SYNC_POINT_CALLBACK("ByteBuffer::allocate_with_tracker", &ret);
        if (ret.ok()) {
            return ptr;
        } else {
            return ret;
        }
#endif
    }

    static StatusOr<ByteBufferPtr> reallocate_with_tracker(const ByteBufferPtr& old_ptr, size_t new_size) {
        if (new_size <= old_ptr->capacity) return old_ptr;

        ASSIGN_OR_RETURN(ByteBufferPtr ptr, allocate_with_tracker(new_size));
        ptr->put_bytes(old_ptr->ptr, old_ptr->pos);
        return ptr;
    }

    ~ByteBuffer() { delete[] ptr; }

    void put_bytes(const char* data, size_t size) {
        strings::memcpy_inlined(ptr + pos, data, size);
        pos += size;
    }

    void get_bytes(char* data, size_t size) {
        strings::memcpy_inlined(data, ptr + pos, size);
        pos += size;
        DCHECK(pos <= limit);
    }

    void flip() {
        limit = pos;
        pos = 0;
    }

    size_t remaining() const { return limit - pos; }
    bool has_remaining() const { return limit > pos; }

    char* const ptr;
    size_t pos{0};
    size_t limit;
    size_t capacity;

private:
    ByteBuffer(size_t capacity_) : ptr(new char[capacity_]), limit(capacity_), capacity(capacity_) {}
};

} // namespace starrocks
