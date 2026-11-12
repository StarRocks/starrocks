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

#include "io/input_stream.h"

namespace starrocks::io {

// The caller's interest in an asynchronous cache fill started by SeekableInputStream::warm_cache_async().
// Dropping the handle withdraws that interest: a block nobody else waits for is not fetched if its
// remote read has not started yet.
class CacheWarmHandle {
public:
    virtual ~CacheWarmHandle() = default;
    // Cache blocks the call queued for fetching; blocks that were cached already are not counted.
    virtual size_t submitted_blocks() const = 0;
    // Queued blocks whose fill has not finished yet.
    virtual size_t pending_blocks() const = 0;
};

class SeekableInputStream : public InputStream {
public:
    ~SeekableInputStream() override = default;

    // Repositions the offset of the InputStream to the argument |position|.
    // If |position| less than zero, returns an error.
    // If |position| greater than the size of the InputStream, the subsequent
    // `read()` will return 0.
    virtual Status seek(int64_t position) = 0;

    // Returns the current offset location as measured in bytes from
    // the beginning of the InputStream
    virtual StatusOr<int64_t> position() = 0;

    // Repositions the offset of the InputStream to |offset| then read |count| bytes.
    //
    // Default implementation:
    // ```
    //    RETURN_IF_ERROR(seek(offset));
    //    return read(out, count);
    // ```
    //
    // Return the number of bytes read, or error.
    virtual StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count);

    // Read exactly the number of |count| bytes data from given position |offset|.
    // This method does not return the number of bytes read because either
    // (1) the entire |count| bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned, leave the content of
    // |data| buffer unspecified.
    //
    // Default implementation:
    // ```
    //    RETURN_IF_ERROR(seek(offset));
    //    return read_fully(out, count);
    // ```
    virtual Status read_at_fully(int64_t offset, void* out, int64_t count);

    // Return the total file size in bytes, or error.
    virtual StatusOr<int64_t> get_size() = 0;

    // Default implementation:
    // ```
    //    ASSIGN_OR_RETURN(auto pos, position());
    //    return seek(pos + count);
    // ```
    Status skip(int64_t count) override;

    virtual void set_size(int64_t);

    // Reads all the data in this stream and returns it as std::string.
    //
    // Some implementations may override this method to get all
    // the data without calling `get_size()`.
    // For example, S3InputStream can read the contents of an entire
    // object directly with a single GET OBJECT call, without the need
    // to first send a HEAD OBJECT request to get the object size.
    virtual StatusOr<std::string> read_all();

    // if cache in [offset, offset+length] exists, refresh it
    // if not, read from remote and write to cache system
    // stream offset will not change
    virtual Status touch_cache(int64_t offset, size_t length) { return Status::OK(); }

    // Starts filling the local cache with the blocks of [offset, offset+length) that are not cached yet,
    // and returns at once; a later read of such a block joins the fill in flight instead of issuing a GET
    // of its own. Only the shared-data cache implements it, and only while the remote I/O scheduler is
    // enabled. Unlike touch_cache(), a stream that cannot do it says so instead of pretending to succeed.
    // `urgent` marks blocks the caller is about to read anyway and only wants fetched in parallel: they are
    // scheduled as a demand, ahead of read-ahead and outside its bounds.
    virtual StatusOr<std::shared_ptr<CacheWarmHandle>> warm_cache_async(int64_t offset, size_t length,
                                                                        bool urgent = false) {
        return Status::NotSupported("warm_cache_async");
    }

    virtual const std::string& filename() const { return _filename; };

    virtual bool is_cache_hit() const { return false; };

    virtual bool is_encrypted() const { return false; };

    // Where this stream's bytes start inside the physical file `filename()` names: non-zero
    // only for a slice view (BundleSeekableInputStream). Callers that hand a stream-relative
    // offset to something working in physical-file coordinates -- the local data cache, which
    // caches the physical object -- must add it.
    virtual int64_t bundle_offset() const { return 0; }

    // Cache key for the page hosted at `stream_offset` bytes within this stream. The default
    // encodes (filename, stream_offset). Streams whose `filename()` is shared with other streams
    // (e.g. BundleSeekableInputStream slices of one physical file) must override to fold their
    // disambiguator into the key, so callers never need to know whether the underlying stream
    // is a slice.
    virtual std::string page_cache_key(int64_t stream_offset) const;

protected:
    std::string _filename = "";
};

class SeekableInputStreamWrapper : public SeekableInputStream {
public:
    explicit SeekableInputStreamWrapper(std::unique_ptr<SeekableInputStream> stream)
            : _impl(stream.release()), _ownership(kTakesOwnership) {}
    explicit SeekableInputStreamWrapper(SeekableInputStream* stream, Ownership ownership)
            : _impl(stream), _ownership(ownership) {}

    ~SeekableInputStreamWrapper() override {
        if (_ownership == kTakesOwnership) delete _impl;
    }

    // Disallow copy and assignment
    SeekableInputStreamWrapper(const SeekableInputStreamWrapper&) = delete;
    void operator=(const SeekableInputStreamWrapper&) = delete;
    // Disallow move
    SeekableInputStreamWrapper(SeekableInputStreamWrapper&&) = delete;
    void operator=(SeekableInputStreamWrapper&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override { return _impl->read(data, count); }

    Status read_fully(void* data, int64_t count) override { return _impl->read_fully(data, count); }

    Status skip(int64_t count) override { return _impl->skip(count); }

    StatusOr<std::string_view> peek(int64_t nbytes) override { return _impl->peek(nbytes); }

    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _impl->get_numeric_statistics();
    }

    IoStatsSnapshot get_io_stats_snapshot() const override;

    StatusOr<int64_t> position() override { return _impl->position(); }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        return _impl->read_at(offset, out, count);
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        return _impl->read_at_fully(offset, out, count);
    }

    StatusOr<int64_t> get_size() override { return _impl->get_size(); }

    Status seek(int64_t offset) override { return _impl->seek(offset); }

    void set_size(int64_t value) override { return _impl->set_size(value); }

    StatusOr<std::string> read_all() override { return _impl->read_all(); }

    bool is_encrypted() const override { return _impl->is_encrypted(); };

    Status touch_cache(int64_t offset, size_t length) override { return _impl->touch_cache(offset, length); }

    StatusOr<std::shared_ptr<CacheWarmHandle>> warm_cache_async(int64_t offset, size_t length,
                                                                bool urgent = false) override {
        return _impl->warm_cache_async(offset, length, urgent);
    }

    // Forwarded, unlike page_cache_key below: the slice base is a property of the stream being
    // wrapped, and every wrapper above it keeps the same coordinates.
    int64_t bundle_offset() const override { return _impl->bundle_offset(); }

    // Intentionally do NOT forward page_cache_key to _impl. Wrappers like RandomAccessFile carry
    // the path on themselves (overriding filename() but leaving _impl's _filename empty), so
    // delegating would lose the name and return a key like (empty, offset). Inheriting the base
    // default makes virtual filename() dispatch back to this wrapper and pick up the right name.

private:
    SeekableInputStream* _impl;
    Ownership _ownership;
};

} // namespace starrocks::io
