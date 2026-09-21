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

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "fs/fs.h"
#include "io/input_stream.h"

namespace starrocks {

class VectorIndexLoadRequest;

// A reader's place on one request, taken while the scheduler lock is held and given back
// when it goes out of scope.
//
// The count of these is what tells the owning thread that somebody may still be writing
// into its buffer, so it is also what keeps the request alive: the owner waits for the
// count to reach zero, then unregisters and returns, destroying the request -- which
// lives on its stack. Handing a worker a bare pointer and counting it only once the read
// began left a window where the request could settle and be destroyed in between. Taking
// the slot is therefore the same step as being handed the request, and holding the slot
// is the licence to touch it.
class VectorIndexReaderSlot {
public:
    VectorIndexReaderSlot() = default;
    // Takes a slot on `r`, or comes back empty when the file already has its full share.
    static VectorIndexReaderSlot try_take(VectorIndexLoadRequest* r, int32_t cap);

    ~VectorIndexReaderSlot();
    VectorIndexReaderSlot(VectorIndexReaderSlot&& other) noexcept : _request(other._request) {
        other._request = nullptr;
    }
    VectorIndexReaderSlot(const VectorIndexReaderSlot&) = delete;
    VectorIndexReaderSlot& operator=(const VectorIndexReaderSlot&) = delete;

    explicit operator bool() const { return _request != nullptr; }
    VectorIndexLoadRequest* operator->() const { return _request; }

private:
    explicit VectorIndexReaderSlot(VectorIndexLoadRequest* r) : _request(r) {}
    VectorIndexLoadRequest* _request = nullptr;
};

// One contiguous read of a `.vi`, split into chunks any worker may claim. The calling
// thread is always one of them, so with no helper this is the single-stream read it
// replaced -- helpers only make it faster.
class VectorIndexLoadRequest {
public:
    VectorIndexLoadRequest(const FileInfo& file_info, const RandomAccessFileOptions& opts, int64_t offset, char* dst,
                           int64_t length, int64_t chunk_bytes);

private:
    friend class VectorIndexLoadScheduler;
    friend class VectorIndexReaderSlot;

    int64_t remaining_chunks() const { return _nchunks - _claimed.load(std::memory_order_relaxed); }
    // Could another reader still help here? A hint only: the cap is enforced by try_join().
    bool could_serve(int32_t cap) const {
        return !_failed.load(std::memory_order_relaxed) && remaining_chunks() > 0 && in_flight() < cap;
    }
    int32_t in_flight() const { return _in_flight.load(std::memory_order_relaxed); }
    int64_t nchunks() const { return _nchunks; }

    // `cap` bounds readers on this file; the calling thread passes kUncapped, since the
    // cap exists to stop helpers piling onto one file, never the thread waiting on it.
    static constexpr int32_t kUncapped = -1;
    bool try_join(int32_t cap);
    void leave();
    // Reads chunks until the range runs out or the read fails. Only a ReaderSlot holder
    // may call this: the slot is what keeps the request alive while the read runs.
    void read_chunks();

    // What this read's streams did, folded in as each reader closes its own.
    const io::IoStatsSnapshot& io_stats() const { return _io; }

    void record_failure(const Status& st);
    void wait_until_settled();

    Status status() const;

    const FileInfo& _file_info;
    const RandomAccessFileOptions& _opts;
    const int64_t _offset;
    char* const _dst;
    const int64_t _length;
    const int64_t _chunk_bytes;
    const int64_t _nchunks;

    std::atomic<int64_t> _claimed{0};   // next chunk index to hand out; may overshoot _nchunks
    std::atomic<int64_t> _completed{0}; // chunks actually read
    std::atomic<int32_t> _in_flight{0}; // readers joined to this request right now
    std::atomic<bool> _failed{false};

    mutable std::mutex _mu;
    std::condition_variable _cv;
    Status _first_error;
    io::IoStatsSnapshot _io;
};

// Bounds how many threads read `.vi` files at once, process-wide. Loads register here
// and a shared set of workers serves them oldest-first, so the concurrency that reaches
// object storage is owned in one place rather than being (concurrent loads x per-load
// width).
class VectorIndexLoadScheduler {
public:
    static VectorIndexLoadScheduler& instance();

    // Reads [offset, offset+length) into `dst`. Blocks until done or failed. `io_out`, if
    // given, accumulates the IO counters of every stream this read used.
    Status read(const FileInfo& file_info, const RandomAccessFileOptions& opts, int64_t offset, char* dst,
                int64_t length, int64_t chunk_bytes, io::IoStatsSnapshot* io_out);

    // Helper tasks allowed at once across all loads. Each load also reads on its own
    // calling thread, which is not drawn from this budget.
    int32_t max_workers() const;

    // Readers allowed on one file at a time; 0 or less means no cap.
    int32_t max_readers_per_file() const;

private:
    void register_request(VectorIndexLoadRequest* r);
    void unregister_request(VectorIndexLoadRequest* r);
    void worker_task();
    void on_worker_exit();
    // Is there a request a new worker could serve? Asks only, unlike pick_locked(): a
    // join nobody drains would stop the request settling, so the plain question needs
    // its own answer.
    bool has_work_locked() const;
    // A slot on the request a worker should take next, or an empty slot when no request
    // can take another reader.
    VectorIndexReaderSlot pick_locked() const;
    VectorIndexReaderSlot pick();
    // Hires up to `wanted` more workers, within the pool's width. Submitting a task is
    // how a worker is hired: the closure carries no work, it goes and picks some.
    void spawn_workers_locked(int64_t wanted);

    mutable std::mutex _mu;
    std::vector<VectorIndexLoadRequest*> _active; // registration order
    // Helper workers alive right now. Capped at the pool's width, so every one of them
    // has a thread and the queue never builds up. Each load's calling thread reads on
    // top of these and is not counted here.
    int32_t _live_workers = 0;
};

} // namespace starrocks
