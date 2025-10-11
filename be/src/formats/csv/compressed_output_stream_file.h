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

#include <memory>

#include "common/status.h"
#include "formats/csv/output_stream.h"
#include "gen_cpp/Types_types.h"
#include "io/async_flush_output_stream.h"
#include "util/raw_container.h"

namespace starrocks::csv {

class CompressedAsyncOutputStreamFile final : public OutputStream {
public:
    CompressedAsyncOutputStreamFile(io::AsyncFlushOutputStream* stream, TCompressionType::type compression_type,
                                    size_t buff_size);

    ~CompressedAsyncOutputStreamFile() override = default;

    Status finalize() override;

    std::size_t size() override { return _stream->tell(); }

protected:
    Status _sync(const char* data, size_t size) override;

private:
    Status _compress_gzip(const char* data, size_t size);

    io::AsyncFlushOutputStream* _stream;
    TCompressionType::type _compression_type;
    raw::RawVector<uint8_t> _compressed_buffer;
};

} // namespace starrocks::csv
