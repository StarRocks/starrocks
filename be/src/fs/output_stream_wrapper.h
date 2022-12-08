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

#include "fs/writable_file_as_stream_buf.h"
#include "fs/writable_file_wrapper.h"

namespace starrocks {

//
// Wrap a WritableFile into std::ostream. Note that no internal buffer
// in the stream.
//
// Example usage:
// #1. Write to file as std::ostream
// ```
//   ASSIGN_OR_RETURN(std::unique_ptr<WritableFile> f, FileSystem::Default()->new_writable_file("a.txt"));
//   OutputStreamWrapper wrapper(f.release(), kTakesOwnership);
//   wrapper << "anything can be sent to std::ostream";
// ```
//
// #2. Serialize protobuf to file directly
// ```
//   TabletMetaPB tablet_meta_pb;
//
//   ASSIGN_OR_RETURN(std::unique_ptr<WritableFile> f, FileSystem::Default()->new_writable_file("a.txt"));
//   OutputStreamWrapper wrapper(f.release(), kTakesOwnership);
//   tablet_meta.SerializeToOStream(&wrapper);
// ```
//
class OutputStreamWrapper final : public WritableFileWrapper, public std::ostream {
public:
    // If |ownership| is kDontTakeOwnership, |file| must outlive this OutputStreamWrapper.
    explicit OutputStreamWrapper(WritableFile* file, Ownership ownership = kDontTakeOwnership)
            : WritableFileWrapper(file, ownership), std::ostream(nullptr), _stream_buf(this) {
        rdbuf(&_stream_buf);
    }

private:
    WritableFileAsStreamBuf _stream_buf;
};

} // namespace starrocks
