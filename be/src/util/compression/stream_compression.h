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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/decompressor.h

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
#include "gen_cpp/types.pb.h"

namespace starrocks {

class StreamCompression {
public:
    virtual ~StreamCompression() = default;

    // implement in derived class
    // input(in):               buf where decompress begin
    // input_len(in):           max length of input buf
    // input_bytes_read(out):   bytes which is consumed by decompressor
    // output(out):             buf where to save decompressed data
    // output_len(in):      max length of output buf
    // output_bytes_written(out):   decompressed data size in output buf
    // stream_end(out):         true if reach the and of stream,
    //                          or normally finished decompressing entire block
    //
    // input and output buf should be allocated and released outside
    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                              size_t output_len, size_t* output_bytes_written, bool* stream_end) = 0;

    virtual Status compress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                            size_t output_len, size_t* output_bytes_written, bool* stream_end) {
        return Status::NotSupported("compress for StreamCompression not supported");
    }

public:
    static Status create_decompressor(CompressionTypePB type, std::unique_ptr<StreamCompression>* decompressor);

    virtual std::string debug_info() { return "StreamCompression"; }

    CompressionTypePB get_type() { return _ctype; }

protected:
    virtual Status init() { return Status::OK(); }

    StreamCompression(CompressionTypePB ctype) : _ctype(ctype) {}

    CompressionTypePB _ctype;
};

} // namespace starrocks
