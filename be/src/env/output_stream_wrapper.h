// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "env/writable_file_as_stream_buf.h"
#include "env/writable_file_wrapper.h"

namespace starrocks {

//
// Wrap a WritableFile into std::ostream. Note that no internal buffer
// in the stream.
//
// Example usage:
// #1. Write to file as std::ostream
// ```
//   std::unique_ptr<WritableFile> f;
//   Env::Default()->new_writable_file("a.txt", &f);
//   OutputStreamWrapper wrapper(f.release(), kTakesOwnership);
//   wrapper << "anything can be sent to std::ostream";
// ```
//
// #2. Serialize protobuf to file directly
// ```
//   TabletMetaPB tablet_meta_pb;
//
//   std::unique_ptr<WritableFile> f;
//   Env::Default()->new_writable_file("a.txt", &f);
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
