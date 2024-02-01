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

#include <butil/macros.h>

#include <cstring>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/spill/block_manager.h"
#include "gen_cpp/types.pb.h"
#include "gutil/macros.h"
#include "util/raw_container.h"

namespace starrocks::spill {
class ChunkBuilder;

enum class SerdeType {
    BY_COLUMN,
};

struct AlignedBuffer {
    AlignedBuffer() = default;

    ~AlignedBuffer() noexcept {
        if (_data) {
            free(_data);
            _data = nullptr;
        }
    }

    DISALLOW_COPY(AlignedBuffer);
    AlignedBuffer(AlignedBuffer&& other) noexcept : _data(other._data), _capacity(other._capacity), _size(other._size) {
        other._data = nullptr;
    }
    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
        if (this != &other) {
            std::swap(_data, other._data);
            std::swap(_capacity, other._capacity);
            std::swap(_size, other._size);
        }
        return *this;
    }

    uint8_t* data() const { return (uint8_t*)_data; }

    void resize(size_t size) {
        const size_t BLOCKSIZE = 4096;
        if (_capacity < size) {
            void* new_data = nullptr;
            if (UNLIKELY(posix_memalign(&new_data, BLOCKSIZE, size) != 0)) {
                throw ::std::bad_alloc();
            }
            if (_data != nullptr) {
                memcpy(new_data, _data, _size);
                free(_data);
            }
            _data = new_data;
            _capacity = size;
        }
        _size = size;
    }

    size_t size() const { return _size; }

private:
    void* _data = nullptr;
    size_t _capacity{};
    size_t _size{};
};

struct SerdeContext {
    AlignedBuffer aligned_buffer;
    std::string serialize_buffer;
};
class Spiller;
// Serde is used to serialize and deserialize spilled data.
class Serde;
using SerdePtr = std::shared_ptr<Serde>;
class Serde {
public:
    Serde(Spiller* parent) : _parent(parent) {}
    virtual ~Serde() = default;

    virtual Status prepare() = 0;
    // serialize chunk and append the serialized data into block
    virtual Status serialize_to_block(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) = 0;
    // deserialize data from block, return the chunk after deserialized
    virtual StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) = 0;

    static StatusOr<SerdePtr> create_serde(Spiller* parent);

protected:
    Spiller* _parent = nullptr;
};

} // namespace starrocks::spill