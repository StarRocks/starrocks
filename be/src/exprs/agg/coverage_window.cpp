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

#include "exprs/agg/coverage_window.h"

#include <atomic>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory_resource>

#include "base/memory/jemalloc_allocator.h"
#include "base/memory/malloc_allocator.h"
#include "column/chunk.h"
#include "column/const_column.h"
#include "column/geo_column.h"
#include "common/config_expr_fwd.h"
#include "exprs/function_context.h"
#include "geo/geo_coverage_simplify.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
// Kernel buffers have an explicit query owner and immediate try_consume, not
// the current worker's batched TLS tracker. Output views retain this allocator.
class QueryAllocator final : public memory::Allocator {
public:
    explicit QueryAllocator(std::shared_ptr<MemTracker> tracker) : tracker(std::move(tracker)) {}
    void* alloc(size_t bytes, size_t alignment = 0) override {
        const auto charged = raw.nallox(bytes);
        if (charged <= 0 || tracker->try_consume(charged)) {
            query_limit.store(true, std::memory_order_relaxed);
            throw std::bad_alloc();
        }
        void* pointer;
        try {
            pointer = raw.alloc(bytes, alignment);
        } catch (...) {
            tracker->release(charged);
            throw;
        }
        if (!pointer) {
            tracker->release(charged);
            throw std::bad_alloc();
        }
        return pointer;
    }
    void* realloc(void*, size_t, size_t, size_t) override { throw std::bad_alloc(); }
    void free(void* pointer, size_t bytes) override {
        raw.free(pointer, bytes);
        tracker->release(raw.nallox(bytes));
    }
    int64_t nallox(size_t bytes, int flags = 0) const override { return raw.nallox(bytes, flags); }
    MemoryKind memory_kind() const override { return raw.memory_kind(); }
    std::shared_ptr<MemTracker> tracker;
    std::atomic<bool> query_limit{false};
    size_t root_bytes = 0;
#if defined(__SANITIZE_ADDRESS__) || defined(ADDRESS_SANITIZER)
    memory::MallocAllocator<false> raw;
#else
    memory::JemallocAllocator<false> raw;
#endif
};
// Query-owner metadata also has a captured tracker. The reporting pointer is
// used only by allocate(), while the object is alive; deallocate() runs after
// the shared object's destruction and uses only the tracker and raw allocator.
template <class T>
struct RootAllocator {
    using value_type = T;
    std::shared_ptr<MemTracker> tracker;
    QueryAllocator* reporting = nullptr;
    RootAllocator(std::shared_ptr<MemTracker> tracker, QueryAllocator* reporting = nullptr)
            : tracker(std::move(tracker)), reporting(reporting) {}
    template <class U>
    RootAllocator(const RootAllocator<U>& other) : tracker(other.tracker), reporting(other.reporting) {}
    T* allocate(size_t n) {
        decltype(QueryAllocator::raw) raw;
        const size_t bytes = n * sizeof(T);
        const auto charged = raw.nallox(bytes);
        if (tracker->try_consume(charged)) throw std::bad_alloc();
        void* pointer = nullptr;
        try {
            pointer = raw.alloc(bytes);
        } catch (...) {
            tracker->release(charged);
            throw;
        }
        if (!pointer) {
            tracker->release(charged);
            throw std::bad_alloc();
        }
        if (reporting) reporting->root_bytes += charged;
        return static_cast<T*>(pointer);
    }
    void deallocate(T* p, size_t n) {
        decltype(QueryAllocator::raw) raw;
        raw.free(p, n * sizeof(T));
        tracker->release(raw.nallox(n * sizeof(T)));
    }
    template <class U>
    bool operator==(const RootAllocator<U>& other) const {
        return tracker == other.tracker;
    }
};
std::shared_ptr<QueryAllocator> query_allocator(std::shared_ptr<MemTracker> tracker) {
    RootAllocator<QueryAllocator> allocator(tracker);
    auto* value = allocator.allocate(1);
    new (value) QueryAllocator(tracker);
    value->root_bytes = value->nallox(sizeof(QueryAllocator));
    return {value,
            [allocator](QueryAllocator* p) mutable {
                p->~QueryAllocator();
                allocator.deallocate(p, 1);
            },
            RootAllocator<QueryAllocator>(tracker, value)};
}
struct Failure {
    Status status;
};
void require(Status status) {
    if (!status.ok()) throw Failure{std::move(status)};
}
Status checkpoint(void* pointer) {
    auto* state = static_cast<RuntimeState*>(pointer);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("ST_CoverageSimplify"));
    return state->query_mem_tracker_ptr()->check_mem_limit("ST_CoverageSimplify");
}
GeoCoverageLimits snapshot() {
    const int64_t rows = config::geo_coverage_max_rows_per_partition;
    const int64_t vertices = config::geo_coverage_max_vertices_per_partition;
    const int64_t input = config::geo_coverage_max_input_bytes_per_partition;
    const int64_t working = config::geo_coverage_max_working_bytes_per_partition;
    if (rows <= 0 || vertices <= 0 || input <= 0 || working <= 0)
        throw Failure{Status::InvalidArgument("ST_CoverageSimplify requires positive geo_coverage limits")};
    return {size_t(rows), size_t(vertices), size_t(input), size_t(working)};
}
struct MemoryOwner final : GeoColumnViewOwner {
    std::shared_ptr<QueryAllocator> allocator;
    std::unique_ptr<GeoCoverageSimplify> core;
    MemoryOwner(std::shared_ptr<QueryAllocator> allocator, std::unique_ptr<GeoCoverageSimplify> core)
            : allocator(std::move(allocator)), core(std::move(core)) {}
    size_t retained_bytes() const override { return core->memory_usage(); }
};
// The allocate_shared control block itself is charged before allocation. Its
// allocator outlives the core, so control-block deallocation has no dangling
// memory_resource dependency when MemoryOwner is destroyed.
template <class T>
struct OwnerAllocator {
    using value_type = T;
    std::shared_ptr<QueryAllocator> allocator;
    GeoCoverageSimplify* admission;
    template <class U>
    OwnerAllocator(const OwnerAllocator<U>& other) : allocator(other.allocator), admission(other.admission) {}
    OwnerAllocator(std::shared_ptr<QueryAllocator> allocator, GeoCoverageSimplify* admission)
            : allocator(std::move(allocator)), admission(admission) {}
    T* allocate(size_t n) {
        require(admission->retain_external(allocator->nallox(n * sizeof(T))));
        return static_cast<T*>(allocator->alloc(n * sizeof(T)));
    }
    void deallocate(T* p, size_t n) { allocator->free(p, n * sizeof(T)); }
    template <class U>
    bool operator==(const OwnerAllocator<U>& other) const {
        return allocator == other.allocator;
    }
};
// Captured owner survives BOTH object destruction and subsequent shared control
// block deallocation. No TLS resource is consulted by a downstream worker.
template <class T>
struct OwnedResourceAllocator {
    using value_type = T;
    std::shared_ptr<MemoryOwner> owner;
    explicit OwnedResourceAllocator(std::shared_ptr<MemoryOwner> owner) : owner(std::move(owner)) {}
    template <class U>
    OwnedResourceAllocator(const OwnedResourceAllocator<U>& other) : owner(other.owner) {}
    T* allocate(size_t n) { return static_cast<T*>(owner->core->resource()->allocate(n * sizeof(T), alignof(T))); }
    void deallocate(T* p, size_t n) { owner->core->resource()->deallocate(p, n * sizeof(T), alignof(T)); }
    template <class U>
    bool operator==(const OwnedResourceAllocator<U>& other) const {
        return owner == other.owner;
    }
};
struct Partition {
    struct Borrowed {
        std::weak_ptr<MemoryOwner> owner;
        size_t charged;
    };
    std::shared_ptr<MemoryOwner> memory;
    std::shared_ptr<Partition> next;
    std::pmr::vector<Borrowed> borrowed;
    size_t emitted = 0;
    bool ready = false;
    explicit Partition(std::shared_ptr<MemoryOwner> memory)
            : memory(std::move(memory)), borrowed(this->memory->core->resource()) {}
    void retain_owner(const std::shared_ptr<MemoryOwner>& owner) {
        if (owner == memory) return;
        for (auto& b : borrowed) {
            if (b.owner.lock() != owner) continue;
            const size_t current = owner->retained_bytes();
            if (current > b.charged) {
                require(memory->core->retain_external(current - b.charged));
                b.charged = current;
            }
            return;
        }
        const size_t current = owner->retained_bytes();
        require(memory->core->retain_external(current));
        borrowed.push_back({owner, current});
    }
    Status refresh_borrowed() {
        try {
            for (auto& b : borrowed)
                if (auto owner = b.owner.lock()) retain_owner(owner);
            return Status::OK();
        } catch (const Failure& failure) {
            return failure.status;
        } catch (...) {
            return Status::MemoryLimitExceeded(
                    "ST_CoverageSimplify exceeds geo_coverage_max_working_bytes_per_partition");
        }
    }
};
struct Backing {
    std::pmr::vector<uint8_t> bytes;
    explicit Backing(std::shared_ptr<MemoryOwner> owner, size_t size) : bytes(owner->core->resource()) {
        // Binary SIMD consumers may read the usual 16 padding bytes.
        bytes.resize(size + 16);
    }
};
struct Frame {
    std::shared_ptr<MemoryOwner> owner;
    std::shared_ptr<Frame> next;
    std::shared_ptr<Backing> backing;
    ColumnPtr column;
    GeoColumn* geo;
    uint32_t* offsets;
    size_t rows, filled = 0, bytes = 0, charged = 0;
    Frame(std::shared_ptr<MemoryOwner> owner, size_t rows, size_t capacity, const GeoTypeDescriptor& descriptor)
            : owner(owner), rows(rows) {
        auto& core = *owner->core;
        // Fixed column objects, native offset/null capacities and CRS copies
        // use the existing column allocators. Precharge their actual size
        // classes before construction; there is no later reserve/growth.
        auto reserve = [&](size_t size) { require(core.retain_external(owner->allocator->nallox(size))); };
        reserve(sizeof(GeoColumn));
        reserve(sizeof(BinaryColumn));
        reserve(sizeof(NullColumn));
        reserve(sizeof(NullableColumn));
        reserve((rows + 1) * sizeof(uint32_t));
        reserve(rows);
        if (descriptor.crs.size() > 15) reserve(descriptor.crs.size() + 1);
        backing = std::allocate_shared<Backing>(OwnedResourceAllocator<Backing>(owner), owner, capacity);
        AdaptiveOffsets positions;
        positions.reserve(rows + 1);
        positions.resize(rows + 1, 0);
        offsets = positions.small_storage().data();
        auto data = GeoColumn::create(
                GeoColumnDescriptor{descriptor,
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}},
                ContainerResource(backing, backing->bytes.data(), capacity), std::move(positions), owner);
        geo = data.get();
        auto nulls = NullColumn::create(rows, 1);
        column = NullableColumn::create(std::move(data), std::move(nulls));
    }
};
const Column* unwrap(const Column* column, size_t& position, bool& null) {
    null = column->only_null();
    if (column->is_constant()) {
        position = 0;
        column = down_cast<const ConstColumn*>(column)->data_column().get();
    }
    if (column->is_nullable()) {
        auto* nullable = down_cast<const NullableColumn*>(column);
        null = null || nullable->is_null(position);
        column = nullable->data_column().get();
    }
    return column;
}
std::optional<double> tolerance(FunctionContext* ctx) {
    if (!ctx->is_constant_column(1))
        throw Failure{Status::InvalidArgument("ST_CoverageSimplify tolerance must be plan-constant")};
    size_t row = 0;
    bool null;
    auto* c = unwrap(ctx->get_constant_column(1).get(), row, null);
    if (null) return std::nullopt;
    auto* values = dynamic_cast<const DoubleColumn*>(c);
    if (!values || values->size() == 0)
        throw Failure{Status::InvalidArgument("ST_CoverageSimplify tolerance requires a DOUBLE constant")};
    double value = values->get_data()[row];
    return value;
}
std::optional<bool> boundary(FunctionContext* ctx) {
    if (ctx->get_arg_types().size() == 2) return true;
    if (!ctx->is_constant_column(2))
        throw Failure{Status::InvalidArgument("ST_CoverageSimplify boundary must be plan-constant")};
    size_t row = 0;
    bool null;
    auto* c = unwrap(ctx->get_constant_column(2).get(), row, null);
    if (null) return std::nullopt;
    auto* values = dynamic_cast<const BooleanColumn*>(c);
    if (!values || values->size() == 0)
        throw Failure{Status::InvalidArgument("ST_CoverageSimplify boundary requires a BOOLEAN constant")};
    return bool(values->get_data()[row]);
}
} // namespace

struct CoverageWindowState::Impl {
    std::shared_ptr<QueryAllocator> allocator;
    std::shared_ptr<Partition> head, tail;
    std::shared_ptr<Frame> frames, last_frame;
    std::optional<double> tolerance;
    std::optional<bool> boundary;
    size_t calls = 0;
    bool initialized = false;
    ~Impl() {
        while (head) {
            auto old = std::move(head);
            head = std::move(old->next);
        }
        tail.reset();
        while (frames) {
            auto old = std::move(frames);
            frames = std::move(old->next);
        }
        last_frame.reset();
    }
};
CoverageWindowState::CoverageWindowState() : impl(std::make_unique<Impl>()) {}
CoverageWindowState::~CoverageWindowState() = default;

Status CoverageWindowFunction::validate_window_plan(FunctionContext* ctx) const {
    try {
        const auto& args = ctx->get_arg_types();
        const auto& result = ctx->get_return_type();
        if ((args.size() != 2 && args.size() != 3) || args[1].type != TYPE_DOUBLE ||
            (args.size() == 3 && args[2].type != TYPE_BOOLEAN))
            return Status::InvalidArgument("ST_CoverageSimplify requires GEOMETRY, DOUBLE and optional BOOLEAN");
        auto planar = [](const TypeDescriptor& t) {
            return t.type == TYPE_GEOMETRY && t.geo_type && t.geo_type->logical_type == GEO_LOGICAL_TYPE_GEOMETRY &&
                   t.geo_type->coordinate_system == GEO_COORDINATE_SYSTEM_CARTESIAN &&
                   t.geo_type->edge_algorithm == GEO_EDGE_ALGORITHM_PLANAR && !t.geo_type->crs.empty();
        };
        if (!planar(args[0]) || !planar(result) || !is_geo_semantically_compatible(*args[0].geo_type, *result.geo_type))
            return Status::InvalidArgument("ST_CoverageSimplify requires compatible planar GEOMETRY CRS descriptors");
        if (!ctx->state() || !ctx->state()->query_mem_tracker_ptr())
            return Status::InvalidArgument("ST_CoverageSimplify requires a query memory owner");
        if (ctx->state()->enable_spill())
            return Status::NotSupported("ST_CoverageSimplify partition state cannot spill; disable enable_spill");
        const auto t = tolerance(ctx);
        const auto b = boundary(ctx);
        if (t && b && (!std::isfinite(*t) || *t < 0 || !std::isfinite(*t * *t)))
            return Status::InvalidArgument(
                    "ST_CoverageSimplify requires finite nonnegative tolerance and "
                    "representable tolerance squared");
        return checkpoint(ctx->state());
    } catch (const Failure& failure) {
        return failure.status;
    }
}

Status CoverageWindowFunction::admit_window_segment(FunctionContext* ctx, AggDataPtr state, const Column* input,
                                                    size_t chunk_rows, size_t start, size_t end, bool new_partition,
                                                    const Chunk* retained_input) const {
    auto& s = *data(state).impl;
    try {
        const auto& args = ctx->get_arg_types();
        const auto& result = ctx->get_return_type();
        RETURN_IF_ERROR(validate_window_plan(ctx));
        if (!input || start >= end || end > chunk_rows || (!input->is_constant() && input->size() < chunk_rows))
            return Status::InvalidArgument("ST_CoverageSimplify invalid input segment");
        size_t first_position = start;
        bool first_null;
        const auto* physical = unwrap(input, first_position, first_null);
        const auto* input_geo = dynamic_cast<const GeoColumn*>(physical);
        if ((!input_geo && !input->only_null()) ||
            (input_geo && (!is_geo_semantically_compatible(input_geo->descriptor().type, *args[0].geo_type) ||
                           input_geo->descriptor().storage.encoding != GEO_ENCODING_WKB ||
                           (input_geo->descriptor().storage.dimension != GEO_DIMENSION_XY &&
                            input_geo->descriptor().storage.dimension != GEO_DIMENSION_UNKNOWN &&
                            input_geo->descriptor().storage.dimension != GEO_DIMENSION_MIXED))))
            return Status::InvalidArgument("ST_CoverageSimplify requires native planar XY GEOMETRY input");
        if (!s.initialized) {
            s.tolerance = tolerance(ctx);
            s.boundary = boundary(ctx);
            s.allocator = query_allocator(ctx->state()->query_mem_tracker_ptr());
            s.initialized = true;
        }
        if (new_partition || !s.tail) {
            auto core = GeoCoverageSimplify::create(s.allocator.get(), snapshot(), {ctx->state(), checkpoint});
            if (!core.ok()) return core.status();
            RETURN_IF_ERROR((*core)->retain_external(s.allocator->root_bytes +
                                                     s.allocator->nallox(sizeof(CoverageWindowState::Impl)) +
                                                     s.allocator->nallox(sizeof(CoverageWindowState))));
            auto* admission = core->get();
            auto owner = std::allocate_shared<MemoryOwner>(OwnerAllocator<MemoryOwner>(s.allocator, admission),
                                                           s.allocator, std::move(core).value());
            auto part = std::allocate_shared<Partition>(OwnedResourceAllocator<Partition>(owner), owner);
            if (s.tail)
                s.tail->next = part;
            else
                s.head = part;
            s.tail = std::move(part);
        }
        auto owner = s.tail->memory;
        auto& core = *owner->core;
        const bool skip = !s.tolerance || !s.boundary;
        for (size_t row = start; row < end; ++row) {
            size_t position = row;
            bool null;
            auto* value = unwrap(input, position, null);
            std::optional<Slice> bytes;
            if (!skip && !null) {
                auto* geo = dynamic_cast<const GeoColumn*>(value);
                if (!geo || !is_geo_semantically_compatible(geo->descriptor().type, *result.geo_type) ||
                    geo->descriptor().storage.encoding != GEO_ENCODING_WKB ||
                    (geo->descriptor().storage.dimension != GEO_DIMENSION_XY &&
                     geo->descriptor().storage.dimension != GEO_DIMENSION_UNKNOWN &&
                     geo->descriptor().storage.dimension != GEO_DIMENSION_MIXED))
                    return Status::InvalidArgument(
                            "ST_CoverageSimplify requires compatible planar XY GEOMETRY descriptors");
                bytes = geo->get_wkb(position);
            }
            RETURN_IF_ERROR(core.append(bytes));
        }
        if (start == 0) {
            size_t capacity = 0;
            if (!skip)
                for (size_t row = 0; row < chunk_rows; ++row) {
                    if (row % 128 == 0) require(checkpoint(ctx->state()));
                    size_t position = row;
                    bool null;
                    auto* value = unwrap(input, position, null);
                    if (!null) {
                        auto* geo = dynamic_cast<const GeoColumn*>(value);
                        if (!geo) return Status::InvalidArgument("ST_CoverageSimplify requires native GEOMETRY input");
                        const size_t bytes = geo->get_wkb(position).size;
                        if (bytes > std::numeric_limits<uint32_t>::max() - capacity)
                            return Status::MemoryLimitExceeded(
                                    "ST_CoverageSimplify output chunk exceeds native offset range");
                        capacity += bytes;
                    }
                }
            const size_t before = core.memory_usage();
            auto frame = std::allocate_shared<Frame>(OwnedResourceAllocator<Frame>(owner), owner, chunk_rows, capacity,
                                                     *result.geo_type);
            frame->charged = core.memory_usage() - before;
            if (s.last_frame)
                s.last_frame->next = frame;
            else
                s.frames = frame;
            s.last_frame = std::move(frame);
        }
        s.tail->retain_owner(s.last_frame->owner);
        if (retained_input)
            for (size_t i = 0; i < retained_input->num_columns(); ++i) {
                require(checkpoint(ctx->state()));
                size_t position = 0;
                bool null;
                const auto* column = retained_input->get_column_by_index(i).get();
                auto* physical = unwrap(column, position, null);
                auto* geo = dynamic_cast<const GeoColumn*>(physical);
                if (!geo) continue;
                bool seen = false;
                for (size_t j = 0; j < i; ++j) {
                    if (j % 128 == 0) require(checkpoint(ctx->state()));
                    size_t previous = 0;
                    bool ignored;
                    seen |= unwrap(retained_input->get_column_by_index(j).get(), previous, ignored) == geo;
                }
                if (!seen) require(core.retain_external(geo->allocation_footprint(*s.allocator)));
                if (column->is_constant()) require(core.retain_external(s.allocator->nallox(sizeof(ConstColumn))));
                if (column->is_nullable()) {
                    require(core.retain_external(s.allocator->nallox(sizeof(NullableColumn))));
                    require(core.retain_external(s.allocator->nallox(sizeof(NullColumn))));
                    const auto* nullable = down_cast<const NullableColumn*>(column);
                    require(core.retain_external(s.allocator->nallox(nullable->null_column()->capacity())));
                }
            }
        return Status::OK();
    } catch (const Failure& failure) {
        return failure.status;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded(s.allocator && s.allocator->query_limit.load()
                                                   ? "ST_CoverageSimplify exceeds query memory limit"
                                                   : "ST_CoverageSimplify window allocation failed");
    } catch (...) {
        return Status::MemoryLimitExceeded("ST_CoverageSimplify exceeds geo_coverage_max_working_bytes_per_partition");
    }
}
MutableColumnPtr CoverageWindowFunction::window_result_column(FunctionContext* ctx, AggDataPtr state) const {
    auto& s = *data(state).impl;
    if (!s.frames) {
        ctx->set_error("ST_CoverageSimplify output frame unavailable");
        return nullptr;
    }
    return s.frames->column->as_mutable_ptr();
}
void CoverageWindowFunction::update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr state,
                                                                  const Column**, int64_t partition_start,
                                                                  int64_t partition_end, int64_t frame_start,
                                                                  int64_t frame_end) const {
    auto& s = *data(state).impl;
    if (!s.head || s.head->ready || frame_start != partition_start || frame_end != partition_end ||
        partition_end - partition_start != s.head->memory->core->rows()) {
        ctx->set_error("ST_CoverageSimplify requires one complete admitted partition");
        return;
    }
    auto& core = *s.head->memory->core;
    auto retained = s.head->refresh_borrowed();
    if (!retained.ok()) {
        ctx->set_error(retained.to_string().c_str());
        return;
    }
    auto status = core.finish(s.tolerance, s.boundary);
    if (!status.ok()) {
        ctx->set_error(status.to_string().c_str());
        return;
    }
    s.calls += core.kernel_calls();
    s.head->ready = true;
}
void CoverageWindowFunction::get_values(FunctionContext* ctx, ConstAggDataPtr state, Column* dst, size_t start,
                                        size_t end) const {
    auto& s = *data(state).impl;
    if (ctx->has_error()) return;
    if (!s.head || !s.head->ready || !s.frames || s.frames->column.get() != dst || s.frames->filled != start ||
        end > s.frames->rows || end - start > s.head->memory->core->rows() - s.head->emitted) {
        ctx->set_error("ST_CoverageSimplify positional window mapping mismatch");
        return;
    }
    auto& frame = *s.frames;
    auto* nullable = down_cast<NullableColumn*>(dst);
    for (size_t row = start; row < end; ++row) {
        if ((row - start) % 128 == 0) {
            auto status = checkpoint(ctx->state());
            if (!status.ok()) {
                ctx->set_error(status.to_string().c_str());
                return;
            }
        }
        auto result = s.head->memory->core->result(s.head->emitted++);
        if (!result.ok()) {
            ctx->set_error(result.status().to_string().c_str());
            return;
        }
        if (*result) {
            const auto bytes = result->value();
            if (bytes.size > frame.backing->bytes.size() - 16 - frame.bytes) {
                ctx->set_error("ST_CoverageSimplify output exceeded admitted capacity");
                return;
            }
            std::memcpy(frame.backing->bytes.data() + frame.bytes, bytes.data, bytes.size);
            frame.bytes += bytes.size;
            nullable->null_column_data()[row] = 0;
        }
        frame.offsets[row + 1] = frame.bytes;
    }
    auto status = checkpoint(ctx->state());
    if (!status.ok()) {
        ctx->set_error(status.to_string().c_str());
        return;
    }
    frame.filled = end;
    if (end == frame.rows) {
        frame.geo->finish_wkb_view(frame.bytes);
        nullable->update_has_null();
        auto old = std::move(s.frames);
        s.frames = std::move(old->next);
        if (!s.frames) s.last_frame.reset();
    }
}
void CoverageWindowFunction::reset(FunctionContext* ctx, const Columns&, AggDataPtr state) const {
    auto& s = *data(state).impl;
    if (!s.head || !s.head->ready) return; // Initial reset precedes materialization.
    if (s.head->emitted != s.head->memory->core->rows()) {
        ctx->set_error("ST_CoverageSimplify cannot reset an incomplete partition");
        return;
    }
    auto old = std::move(s.head);
    s.head = std::move(old->next);
    if (!s.head) s.tail.reset();
}
size_t CoverageWindowFunction::kernel_calls(ConstAggDataPtr state) const {
    return data(state).impl->calls;
}
} // namespace starrocks
