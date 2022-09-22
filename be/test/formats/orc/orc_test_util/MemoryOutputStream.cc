// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Inc.

// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/test/MemoryOutputStream.cc

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "MemoryOutputStream.hh"

namespace starrocks::vectorized {

MemoryOutputStream::~MemoryOutputStream() {
    delete[] data;
}

void MemoryOutputStream::write(const void* buf, size_t size) {
    memcpy(data + length, buf, size);
    length += size;
}
} // namespace starrocks::vectorized
