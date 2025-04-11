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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/slice.h

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

#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "io/cache_input_stream.h"

namespace starrocks {

class Filler {
public:
    virtual ~Filler() = default;
    virtual StatusOr<size_t> fill(char* data, size_t length, size_t limit) = 0;
};

class DecompressFiller : public Filler {
public:
    DecompressFiller(std::unique_ptr<io::CompressedInputStream> source_stream): _source_stream(std::move(source_stream)) {}
    StatusOr<size_t> fill(char* data, size_t length, size_t limit) override {
        return _source_stream->fill(data, length, limit);
    }

private:
    std::unique_ptr<io::CompressedInputStream> _source_stream;
};

class LazySlice {
public:
    LazySlice(char* data, size_t size, size_t filled, std::unique_ptr<Filler> filler) : _data(data), _size(size), _filled(filled), _filler(std::move(filler)) {}

    Status try_to_trigger_fill(size_t offset, size_t length) {
        if (offset + length > _filled) {
            ASSIGN_OR_RETURN(auto fill_size, _filler->fill(_data + _filled, length, _size - _filled));
            _filled += fill_size;
        }
        return Status::OK();
    }

private:
    char* _data;
    size_t _size = 0;
    size_t _filled = 0;
    std::unique_ptr<Filler> _filler;
};

} // starrocks