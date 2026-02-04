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

#include "base/testutil/sync_point.h"
#include "common/logging.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/utils.h"

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

enum class ByteBufferMetaType { NONE, KAFKA };

std::string byte_buffer_meta_type_name(ByteBufferMetaType type);

class ByteBufferMeta {
public:
    virtual ~ByteBufferMeta() = default;
    virtual ByteBufferMetaType type() = 0;
    virtual Status copy_from(ByteBufferMeta* source) = 0;
    virtual std::string to_string() = 0;

    static StatusOr<ByteBufferMeta*> create(ByteBufferMetaType meta_type);
};

class NoneByteBufferMeta : public ByteBufferMeta {
public:
    NoneByteBufferMeta(const NoneByteBufferMeta&) = delete;
    NoneByteBufferMeta& operator=(const NoneByteBufferMeta&) = delete;
    NoneByteBufferMeta(NoneByteBufferMeta&&) = delete;
    NoneByteBufferMeta& operator=(NoneByteBufferMeta&&) = delete;

    ByteBufferMetaType type() override { return ByteBufferMetaType::NONE; }

    Status copy_from(ByteBufferMeta* source) override;
    std::string to_string() override { return "none"; }

    static NoneByteBufferMeta* instance() {
        static NoneByteBufferMeta instance;
        return &instance;
    }

private:
    NoneByteBufferMeta() = default;
};

class KafkaByteBufferMeta : public ByteBufferMeta {
public:
    KafkaByteBufferMeta() = default;

    ByteBufferMetaType type() override { return ByteBufferMetaType::KAFKA; }

    void set_partition(int32_t partition) { _partition = partition; }
    int32_t partition() const { return _partition; }
    void set_offset(int64_t offset) { _offset = offset; }
    int64_t offset() const { return _offset; }

    Status copy_from(ByteBufferMeta* source) override;

    std::string to_string() override { return fmt::format("kafka partition: {}, offset: {}", _partition, _offset); }

private:
    int32_t _partition{-1};
    int64_t _offset{-1};
};

struct ByteBuffer {
    static StatusOr<ByteBufferPtr> allocate_with_tracker(size_t size, size_t padding = 0,
                                                         ByteBufferMetaType meta_type = ByteBufferMetaType::NONE) {
        auto tracker = CurrentThread::mem_tracker();
        if (tracker == nullptr) {
            return Status::InternalError("current thread memory tracker Not Found when allocate ByteBuffer");
        }
#ifndef BE_TEST
        // check limit before allocation
        TRY_CATCH_BAD_ALLOC({
            ASSIGN_OR_RETURN(auto meta, ByteBufferMeta::create(meta_type));
            // if allocate buffer failed, meta will be deleted
            DeferOp defer([&]() { delete_meta_safely(meta); });
            ByteBufferPtr ptr(new ByteBuffer(size, padding, meta), MemTrackerDeleter(tracker));
            // set meta to nullptr to avoid being deleted
            meta = nullptr;
            return ptr;
        });
#else
        ASSIGN_OR_RETURN(auto meta, ByteBufferMeta::create(meta_type));
        ByteBufferPtr ptr(new ByteBuffer(size, padding, meta), MemTrackerDeleter(tracker));
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
        size_t new_capacity = new_size + old_ptr->padding;
        if (new_capacity <= old_ptr->capacity) return old_ptr;

        ASSIGN_OR_RETURN(ByteBufferPtr ptr, allocate_with_tracker(new_size, old_ptr->padding, old_ptr->meta()->type()));
        ptr->put_bytes(old_ptr->ptr, old_ptr->pos);
        RETURN_IF_ERROR(ptr->meta()->copy_from(old_ptr->meta()));
        return ptr;
    }

    ~ByteBuffer() {
        delete[] ptr;
        delete_meta_safely(_meta);
    }

    void put_bytes(const char* data, size_t size) {
        strings::memcpy_inlined(ptr + pos, data, size);
        pos += size;
    }

    void get_bytes(char* data, size_t size) {
        strings::memcpy_inlined(data, ptr + pos, size);
        pos += size;
        DCHECK(pos <= limit);
    }

    void flip_to_read() {
        limit = pos;
        pos = 0;
    }

    void flip_to_write() {
        if (pos > 0) {
            if (has_remaining()) {
                size_t size = remaining();
                std::memmove(ptr, ptr + pos, size);
                pos = size;
            } else {
                pos = 0;
            }
        } else {
            pos = limit;
        }
        limit = capacity - padding;
    }

    size_t remaining() const { return limit - pos; }
    bool has_remaining() const { return limit > pos; }

    ByteBufferMeta* meta() const { return _meta; }

    char* write_ptr() const { return ptr + pos; }

    char* const ptr;
    size_t pos{0};
    size_t limit;
    const size_t padding;
    const size_t capacity;

private:
    ByteBuffer(size_t size_, size_t padding_ = 0, ByteBufferMeta* meta = NoneByteBufferMeta::instance())
            : ptr(new char[size_ + padding_]),
              limit(size_),
              padding(padding_),
              capacity(size_ + padding_),
              _meta(meta) {
        DCHECK(_meta != nullptr);
    };

    static void delete_meta_safely(ByteBufferMeta* meta) {
        if (meta != nullptr && meta != NoneByteBufferMeta::instance()) {
            delete meta;
        }
    }

    ByteBufferMeta* _meta;
};

} // namespace starrocks
