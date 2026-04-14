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

#include <arrow-adbc/adbc.h>

namespace starrocks {

// Move-only RAII wrapper for ArrowArrayStream.
// Calls stream.release(&stream) on destruction if release callback is set.
struct ArrowArrayStreamHolder {
    ArrowArrayStream stream{};

    ArrowArrayStreamHolder() { stream.release = nullptr; }
    ~ArrowArrayStreamHolder() {
        if (stream.release) {
            stream.release(&stream);
            stream.release = nullptr;
        }
    }

    ArrowArrayStreamHolder(const ArrowArrayStreamHolder&) = delete;
    ArrowArrayStreamHolder& operator=(const ArrowArrayStreamHolder&) = delete;

    ArrowArrayStreamHolder(ArrowArrayStreamHolder&& o) noexcept : stream(o.stream) { o.stream.release = nullptr; }
    ArrowArrayStreamHolder& operator=(ArrowArrayStreamHolder&& o) noexcept {
        if (this != &o) {
            if (stream.release) stream.release(&stream);
            stream = o.stream;
            o.stream.release = nullptr;
        }
        return *this;
    }

    ArrowArrayStream* get() { return &stream; }
    const ArrowArrayStream* get() const { return &stream; }
};

// Move-only RAII wrapper for ArrowSchema.
struct ArrowSchemaHolder {
    ArrowSchema schema{};

    ArrowSchemaHolder() { schema.release = nullptr; }
    ~ArrowSchemaHolder() {
        if (schema.release) {
            schema.release(&schema);
            schema.release = nullptr;
        }
    }

    ArrowSchemaHolder(const ArrowSchemaHolder&) = delete;
    ArrowSchemaHolder& operator=(const ArrowSchemaHolder&) = delete;

    ArrowSchemaHolder(ArrowSchemaHolder&& o) noexcept : schema(o.schema) { o.schema.release = nullptr; }
    ArrowSchemaHolder& operator=(ArrowSchemaHolder&& o) noexcept {
        if (this != &o) {
            if (schema.release) schema.release(&schema);
            schema = o.schema;
            o.schema.release = nullptr;
        }
        return *this;
    }

    ArrowSchema* get() { return &schema; }
};

// Move-only RAII wrapper for ArrowArray.
struct ArrowArrayHolder {
    ArrowArray array{};

    ArrowArrayHolder() { array.release = nullptr; }
    ~ArrowArrayHolder() {
        if (array.release) {
            array.release(&array);
            array.release = nullptr;
        }
    }

    ArrowArrayHolder(const ArrowArrayHolder&) = delete;
    ArrowArrayHolder& operator=(const ArrowArrayHolder&) = delete;

    ArrowArrayHolder(ArrowArrayHolder&& o) noexcept : array(o.array) { o.array.release = nullptr; }
    ArrowArrayHolder& operator=(ArrowArrayHolder&& o) noexcept {
        if (this != &o) {
            if (array.release) array.release(&array);
            array = o.array;
            o.array.release = nullptr;
        }
        return *this;
    }

    ArrowArray* get() { return &array; }
};

} // namespace starrocks
