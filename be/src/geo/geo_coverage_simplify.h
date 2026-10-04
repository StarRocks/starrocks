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
#include <memory_resource>
#include <optional>

#include "base/memory/memory_allocator.h"
#include "base/string/slice.h"
#include "common/statusor.h"

namespace starrocks {

struct GeoCoverageLimits {
    size_t rows = 10000;
    size_t vertices = 1000000;
    size_t input_bytes = 67108864;
    size_t working_bytes = 268435456;
    // A defensive bound on the kernel's own operations, not a time guarantee.
    size_t work = 100000000;
};

// Non-owning callback/context: the query owns that context for the partition
// lifetime. No opaque heap-allocated std::function bypasses the memory budget.
struct GeoCoverageCheckpoint {
    void* context = nullptr;
    Status (*check)(void*) = nullptr;
};

// One complete partition. Admission precedes payload copies/decode; no row is
// available until finish succeeds. Holds original and output native WKB with
// positional mapping, including NULL/EMPTY and MULTIPOLYGON members. The caller
// owns descriptor compatibility and must count any additionally retained column
// backing through retain_external before that backing is allocated/retained.
// The supplied allocator must report allocation size classes through nallox and
// outlive this object. All variable-size kernel storage uses its bounded wrapper;
// allocations and frees retain that owner across sequential worker migration.
class GeoCoverageSimplify {
public:
    static StatusOr<std::unique_ptr<GeoCoverageSimplify>> create(memory::Allocator* allocator,
                                                                 GeoCoverageLimits limits = {},
                                                                 GeoCoverageCheckpoint checkpoint = {});
    ~GeoCoverageSimplify();
    static void operator delete(void* pointer) noexcept;

    Status append(std::optional<Slice> wkb);
    Status finish(std::optional<double> tolerance, std::optional<bool> simplify_boundary = true);
    StatusOr<std::optional<Slice>> result(size_t position) const;
    // Contiguous immutable output, valid only after a successful finish. Used
    // by the native window adapter without a second WKB payload copy. Keep this
    // partition alive while borrowing it or allocating through its resource.
    StatusOr<Slice> result_buffer() const;
    std::pmr::memory_resource* resource();

    // Reservation for caller-owned retained input/output capacity. Uses bytes
    // reported by the actual allocator, including reallocation peaks. Release
    // only when that storage is no longer retained by this partition.
    Status retain_external(size_t bytes);
    void release_external(size_t bytes);
    size_t rows() const;
    size_t memory_usage() const;
    size_t peak_memory_usage() const;
    size_t kernel_calls() const;

private:
    struct Impl;
    explicit GeoCoverageSimplify(Impl* impl) : _impl(impl) {}
    Impl* _impl;
};

} // namespace starrocks
