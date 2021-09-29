// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/memory/buffer.cpp

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

#include "storage/memory/buffer.h"

namespace starrocks::memory {

Status Buffer::alloc(size_t bsize) {
    if (bsize > 0) {
        uint8_t* data = reinterpret_cast<uint8_t*>(aligned_malloc(bsize, bsize >= 4096 ? 4096 : 64));
        if (!data) {
            return Status::MemoryAllocFailed(StringPrintf("alloc buffer size=%zu failed", bsize));
        }
        _data = data;
        _bsize = bsize;
    }
    return Status::OK();
}

void Buffer::clear() {
    if (_data) {
        free(_data);
        _data = nullptr;
        _bsize = 0;
    }
}

void Buffer::set_zero() {
    if (_data) {
        memset(_data, 0, _bsize);
    }
}

Buffer::~Buffer() {
    clear();
}

} // namespace starrocks::memory
