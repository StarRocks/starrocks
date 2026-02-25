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

#include <aws/core/utils/stream/PreallocatedStreamBuf.h>

namespace starrocks::io {

#define STRINGIZE_DETAIL(x) #x
#define STRINGIZE(x) STRINGIZE_DETAIL(x)
#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIZE(__LINE__)

class S3ZeroCopyIOStream : public Aws::IOStream {
public:
    S3ZeroCopyIOStream(char* buf, size_t size)
            : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(buf), size)),
              _buf_size(size) {
        DCHECK(rdbuf());
    }

    S3ZeroCopyIOStream(const char* buf, size_t size) : S3ZeroCopyIOStream(const_cast<char*>(buf), size) {}

    size_t getSize() { return _buf_size; }

    S3ZeroCopyIOStream(const S3ZeroCopyIOStream&) = delete;
    void operator=(const S3ZeroCopyIOStream&) = delete;
    S3ZeroCopyIOStream(S3ZeroCopyIOStream&&) = delete;
    void operator=(S3ZeroCopyIOStream&&) = delete;

    ~S3ZeroCopyIOStream() {
        // corresponding new in constructor
        delete rdbuf();
    }

private:
    size_t _buf_size = 0;
};

} // namespace starrocks::io