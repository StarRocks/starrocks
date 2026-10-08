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

#include "geo/geo_coverage_simplify.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <memory_resource>
#include <numeric>
#include <vector>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include "types/geo_wkb.h"

namespace starrocks {
namespace {
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;
constexpr uint32_t kAbsent = std::numeric_limits<uint32_t>::max();
using Key = std::pair<uint32_t, uint32_t>;
struct Stop {
    Status status;
};
// Lives independently before the public object and survives its destructor.
// The entire state/public-object allocation has one captured allocator owner.
struct StateAllocation {
    memory::Allocator* allocator;
    void* raw;
    size_t bytes;
};
[[noreturn]] void invalid(const char* message) {
    throw Stop{Status::InvalidArgument(message)};
}

// Bounded individually freed allocations, including temporary peaks. An aligned
// view retains its raw allocation header; accounting uses the underlying size
// class before alloc, not requested WKB length or a model expansion estimate.
class Memory final : public std::pmr::memory_resource {
public:
    Memory(memory::Allocator* allocator, size_t limit) : allocator(allocator), limit(limit) {}
    void retain(size_t count) {
        size_t current = used.load(std::memory_order_relaxed);
        do {
            if (count > limit - current)
                throw Stop{Status::MemoryLimitExceeded(
                        "ST_CoverageSimplify exceeds geo_coverage_max_working_bytes_per_partition")};
        } while (!used.compare_exchange_weak(current, current + count, std::memory_order_relaxed));
        size_t previous = peak.load(std::memory_order_relaxed);
        while (previous < current + count &&
               !peak.compare_exchange_weak(previous, current + count, std::memory_order_relaxed)) {
        }
    }
    void release(size_t count) { used.fetch_sub(count, std::memory_order_relaxed); }
    memory::Allocator* allocator;
    size_t limit;
    // The algorithm is single-owner. Immutable native output buffers can be
    // released by independent downstream workers after it has finished.
    std::atomic<size_t> used{0}, peak{0};

private:
    struct Header {
        void* raw;
        size_t requested;
        size_t charged;
    };
    void* do_allocate(size_t count, size_t alignment) override {
        if (alignment > std::numeric_limits<size_t>::max() - sizeof(Header) ||
            count > std::numeric_limits<size_t>::max() - alignment - sizeof(Header))
            throw std::bad_alloc();
        const size_t request = count + alignment + sizeof(Header);
        const int64_t size_class = allocator->nallox(request);
        if (size_class <= 0 || uint64_t(size_class) < request) throw std::bad_alloc();
        retain(size_class);
        void* raw;
        try {
            raw = allocator->alloc(request);
        } catch (...) {
            release(size_class);
            throw;
        }
        if (raw == nullptr) {
            release(size_class);
            throw std::bad_alloc();
        }
        void* aligned = static_cast<char*>(raw) + sizeof(Header);
        size_t space = request - sizeof(Header);
        if (std::align(alignment, count, aligned, space) == nullptr) {
            allocator->free(raw, request);
            release(size_class);
            throw std::bad_alloc();
        }
        auto* header = static_cast<Header*>(aligned) - 1;
        *header = {raw, request, size_t(size_class)};
        return aligned;
    }
    void do_deallocate(void* pointer, size_t, size_t) override {
        auto header = *(static_cast<Header*>(pointer) - 1);
        allocator->free(header.raw, header.requested);
        release(header.charged);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};

// Only default construction needs the scoped resource. Each allocator instance
// captures its owner, including multiprecision rebound/temporary allocations;
// later frees never consult this TLS variable. No simultaneous mutable use of a
// partition is allowed, but sequential movement between workers is supported.
thread_local std::pmr::memory_resource* construction_memory = nullptr;
struct MemoryScope {
    std::pmr::memory_resource* previous;
    explicit MemoryScope(Memory& memory) : previous(construction_memory) { construction_memory = &memory; }
    ~MemoryScope() { construction_memory = previous; }
};
template <class T>
struct Alloc {
    using value_type = T;
    std::pmr::memory_resource* memory = construction_memory;
    Alloc() = default;
    explicit Alloc(std::pmr::memory_resource* m) : memory(m) {}
    template <class U>
    Alloc(const Alloc<U>& other) : memory(other.memory) {}
    T* allocate(size_t n) {
        if (!memory || n > std::numeric_limits<size_t>::max() / sizeof(T)) throw std::bad_alloc();
        return static_cast<T*>(memory->allocate(n * sizeof(T), alignof(T)));
    }
    void deallocate(T* pointer, size_t n) { memory->deallocate(pointer, n * sizeof(T), alignof(T)); }
    template <class U>
    bool operator==(const Alloc<U>& other) const {
        return memory == other.memory;
    }
    template <class U>
    bool operator!=(const Alloc<U>& other) const {
        return !(*this == other);
    }
};
// Finite binary64 scaled by 2^1074 has <2098 bits; determinants <4199,
// ring sums with at most 1M positions <4219. 16384 checked bits also cover
// tolerance^2 and doubled midpoint samples. Dynamic limbs use the budget.
using Integer = boost::multiprecision::number<
        boost::multiprecision::cpp_int_backend<128, 16384, boost::multiprecision::signed_magnitude,
                                               boost::multiprecision::checked, Alloc<boost::multiprecision::limb_type>>,
        boost::multiprecision::et_off>;
Integer scaled(double value) {
    const uint64_t bits = std::bit_cast<uint64_t>(value);
    const unsigned exponent = (bits >> 52) & 2047;
    Integer result = bits & ((uint64_t{1} << 52) - 1);
    if (exponent) {
        result += uint64_t{1} << 52;
        result <<= exponent - 1;
    }
    return bits >> 63 ? -result : result;
}
int sign(const Integer& value) {
    return (value > 0) - (value < 0);
}
struct Point {
    double x, y;
    Integer ex, ey;
};
Integer orientation(const Point& a, const Point& b, const Point& c) {
    return (b.ex - a.ex) * (c.ey - a.ey) - (b.ey - a.ey) * (c.ex - a.ex);
}
Key key(uint32_t a, uint32_t b) {
    return std::minmax(a, b);
}
// Wider exponent range prevents box split/area overflow at binary64 extremes.
// Index boxes only select candidates; all topology decisions are exact.
using BoxPoint = bg::model::d2::point_xy<long double>;
using Box = bg::model::box<BoxPoint>;
using Item = std::pair<Box, uint32_t>;
using Tree = bgi::rtree<Item, bgi::quadratic<16>, bgi::indexable<Item>, bgi::equal_to<Item>, Alloc<Item>>;
// query() traverses recursively with stack storage. qbegin() in Boost 1.80
// instead owns an uncustomizable std::vector path stack, so it is not used.
template <class F>
struct QueryOutput {
    F* visitor;
    QueryOutput& operator*() { return *this; }
    QueryOutput& operator++() { return *this; }
    QueryOutput operator++(int) { return *this; }
    QueryOutput& operator=(const Item& item) {
        (*visitor)(item);
        return *this;
    }
};
template <class F>
void query(const Tree& tree, const Box& box, F&& visitor) {
    tree.query(bgi::intersects(box), QueryOutput<std::remove_reference_t<F>>{&visitor});
}
Box bounds(const Point& a, const Point& b) {
    return {{std::min(a.x, b.x), std::min(a.y, b.y)}, {std::max(a.x, b.x), std::max(a.y, b.y)}};
}
bool between(const Point& p, const Point& a, const Point& b) {
    return std::min(a.x, b.x) <= p.x && p.x <= std::max(a.x, b.x) && std::min(a.y, b.y) <= p.y &&
           p.y <= std::max(a.y, b.y);
}
enum class Contact { NONE, ENDPOINT, TJUNCTION, CROSS, OVERLAP };
Contact contact(const Point& a, const Point& b, const Point& c, const Point& d) {
    const int ac = sign(orientation(a, b, c)), ad = sign(orientation(a, b, d));
    const int ca = sign(orientation(c, d, a)), cb = sign(orientation(c, d, b));
    if (ac * ad < 0 && ca * cb < 0) return Contact::CROSS;
    if (ac == 0 && ad == 0 && ca == 0 && cb == 0) {
        auto less = [](const Point& p, const Point& q) { return p.x < q.x || (p.x == q.x && p.y < q.y); };
        const auto& lo1 = less(a, b) ? a : b;
        const auto& hi1 = less(a, b) ? b : a;
        const auto& lo2 = less(c, d) ? c : d;
        const auto& hi2 = less(c, d) ? d : c;
        const auto& lo = less(lo1, lo2) ? lo2 : lo1;
        const auto& hi = less(hi1, hi2) ? hi1 : hi2;
        if (less(hi, lo)) return Contact::NONE;
        if (less(lo, hi)) return Contact::OVERLAP;
    }
    auto equal = [](const Point& p, const Point& q) { return p.x == q.x && p.y == q.y; };
    if ((ac == 0 && between(c, a, b) && !equal(c, a) && !equal(c, b)) ||
        (ad == 0 && between(d, a, b) && !equal(d, a) && !equal(d, b)) ||
        (ca == 0 && between(a, c, d) && !equal(a, c) && !equal(a, d)) ||
        (cb == 0 && between(b, c, d) && !equal(b, c) && !equal(b, d)))
        return Contact::TJUNCTION;
    if ((ac == 0 && between(c, a, b)) || (ad == 0 && between(d, a, b)) || (ca == 0 && between(a, c, d)) ||
        (cb == 0 && between(b, c, d)))
        return Contact::ENDPOINT;
    return Contact::NONE;
}

struct Occurrence {
    uint32_t node, ring, previous, next;
    bool active = true;
};
struct Ring {
    uint32_t first, count, component;
    bool hole;
    Integer area;
};
struct Component {
    uint32_t row, first, count;
};
struct Node {
    Point point;
    std::pmr::vector<uint32_t> occurrences;
    uint32_t version = 0;
    bool fixed = false, active = true;
    Node(Point p, Memory& m) : point(std::move(p)), occurrences(&m) {}
};
struct Edge {
    uint32_t a, b;
    std::array<uint32_t, 2> owners{};
    unsigned count = 0;
    bool active = true;
};
struct Row {
    std::pmr::vector<uint8_t> bytes;
    size_t output_first = 0, output_size = 0;
    uint32_t first = 0, count = 0;
    bool null = false, multi = false;
    explicit Row(Memory& m) : bytes(&m) {}
};
struct Candidate {
    Integer area;
    uint32_t node, version;
};
struct CandidateLess {
    bool operator()(const Candidate& a, const Candidate& b) const {
        return a.area != b.area ? a.area > b.area : a.node > b.node;
    }
};

// Native OGC WKB access only, with structural preflight before payload copies.
class Reader {
public:
    explicit Reader(Slice bytes) : bytes(bytes) {}
    template <class T>
    T number(bool little) {
        if (sizeof(T) > bytes.size - position) invalid("ST_CoverageSimplify truncated WKB");
        T result;
        memcpy(&result, bytes.data + position, sizeof(T));
        position += sizeof(T);
        if (little != (std::endian::native == std::endian::little)) {
            auto* raw = reinterpret_cast<uint8_t*>(&result);
            std::reverse(raw, raw + sizeof(T));
        }
        return result;
    }
    std::pair<bool, uint32_t> header() {
        uint8_t order = number<uint8_t>(true);
        if (order > 1) invalid("ST_CoverageSimplify invalid WKB byte order");
        return {order == 1, number<uint32_t>(order == 1)};
    }
    void done() const {
        if (position != bytes.size) invalid("ST_CoverageSimplify trailing WKB bytes");
    }
    Slice bytes;
    size_t position = 0;
    size_t coordinates = 0;
    size_t components = 0;
};
template <class T>
void write(std::pmr::vector<uint8_t>& output, T value) {
    if constexpr (std::endian::native != std::endian::little) {
        auto* raw = reinterpret_cast<uint8_t*>(&value);
        std::reverse(raw, raw + sizeof(T));
    }
    const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
    output.insert(output.end(), bytes, bytes + sizeof(T));
}
} // namespace

struct GeoCoverageSimplify::Impl {
    Memory memory;
    GeoCoverageLimits limits;
    GeoCoverageCheckpoint checkpoint;
    std::pmr::vector<Row> rows;
    std::pmr::vector<uint8_t> output_bytes;
    std::pmr::vector<Node> nodes;
    std::pmr::vector<Occurrence> occurrences;
    std::pmr::vector<Ring> rings;
    std::pmr::vector<Component> components;
    std::pmr::map<std::pair<double, double>, uint32_t> node_map;
    std::pmr::map<Key, uint32_t> edge_map;
    std::pmr::vector<Edge> edges;
    std::pmr::vector<Candidate> heap;
    Tree edge_tree, node_tree;
    size_t input_bytes = 0, vertices = 0, work = 0, last_check = 0, calls = 0, external = 0;
    bool finished = false, failed = false, null_parameter = false;
    Impl(memory::Allocator* allocator, GeoCoverageLimits limits, GeoCoverageCheckpoint checkpoint, size_t metadata)
            : memory(allocator, limits.working_bytes),
              limits(limits),
              checkpoint(std::move(checkpoint)),
              rows(&memory),
              output_bytes(&memory),
              nodes(&memory),
              occurrences(&memory),
              rings(&memory),
              components(&memory),
              node_map(&memory),
              edge_map(&memory),
              edges(&memory),
              heap(&memory),
              edge_tree(bgi::quadratic<16>(), bgi::indexable<Item>(), bgi::equal_to<Item>(), Alloc<Item>(&memory)),
              node_tree(bgi::quadratic<16>(), bgi::indexable<Item>(), bgi::equal_to<Item>(), Alloc<Item>(&memory)) {
        memory.retain(metadata);
    }
    void check() {
        last_check = work;
        if (checkpoint.check) {
            auto status = checkpoint.check(checkpoint.context);
            if (!status.ok()) throw Stop{std::move(status)};
        }
    }
    void tick(size_t count = 1) {
        if (count > limits.work - work) invalid("ST_CoverageSimplify exceeds kernel work limit");
        work += count;
        if (work - last_check >= 128) check();
    }
    void preflight(Reader& reader, bool child, size_t& count) {
        check();
        const GeoWkbLimits native_limits;
        if (reader.bytes.size > native_limits.max_bytes)
            invalid("ST_CoverageSimplify exceeds native WKB payload limit");
        if (reader.components == native_limits.max_components)
            invalid("ST_CoverageSimplify exceeds native WKB component limit");
        ++reader.components;
        auto [little, type] = reader.header();
        if (type != 3 && (type != 6 || child)) invalid("ST_CoverageSimplify requires XY POLYGON/MULTIPOLYGON WKB");
        uint32_t n = reader.number<uint32_t>(little);
        if (type == 3) {
            if (n > native_limits.max_components - reader.components)
                invalid("ST_CoverageSimplify exceeds native WKB component limit");
            reader.components += n;
        }
        // Every declared ring/child needs at least its count/header bytes.
        if (n > (reader.bytes.size - reader.position) / (type == 3 ? 4 : 9))
            invalid("ST_CoverageSimplify truncated WKB components");
        for (uint32_t i = 0; i < n; ++i) {
            tick();
            if (type == 6) {
                preflight(reader, true, count);
                continue;
            }
            const uint32_t positions = reader.number<uint32_t>(little);
            if (positions > native_limits.max_coordinates - reader.coordinates)
                invalid("ST_CoverageSimplify exceeds native WKB coordinate limit");
            reader.coordinates += positions;
            if (positions > limits.vertices - count)
                invalid("ST_CoverageSimplify exceeds geo_coverage_max_vertices_per_partition");
            if (positions > (reader.bytes.size - reader.position) / 16)
                invalid("ST_CoverageSimplify truncated WKB ring");
            count += positions;
            for (uint32_t j = 0; j < positions; ++j) {
                tick();
                double x = reader.number<double>(little), y = reader.number<double>(little);
                if (!std::isfinite(x) || !std::isfinite(y)) invalid("ST_CoverageSimplify nonfinite XY coordinate");
            }
        }
    }
    uint32_t intern(double x, double y) {
        auto [it, fresh] = node_map.emplace(std::make_pair(x, y), nodes.size());
        if (fresh) nodes.emplace_back(Point{x, y, scaled(x), scaled(y)}, memory);
        return it->second;
    }
    void polygon(Reader& reader, uint32_t row, bool little) {
        uint32_t n = reader.number<uint32_t>(little), component = components.size();
        components.push_back({row, uint32_t(rings.size()), n});
        for (uint32_t r = 0; r < n; ++r) {
            const uint32_t positions = reader.number<uint32_t>(little), ring = rings.size();
            if (positions < 4) invalid("ST_CoverageSimplify degenerate polygon ring");
            uint32_t first = kAbsent, previous = kAbsent, first_node = kAbsent, last_node = kAbsent;
            for (uint32_t j = 0; j < positions; ++j) {
                tick();
                double x = reader.number<double>(little), y = reader.number<double>(little);
                const uint32_t node = intern(x, y);
                if (j == 0) first_node = node;
                if (j + 1 == positions) {
                    if (node != first_node) invalid("ST_CoverageSimplify unclosed polygon ring");
                    break;
                }
                // Consecutive repeated positions contribute to admission, but
                // a zero-length edge is not a distinct coverage segment.
                if (node == last_node) continue;
                uint32_t occurrence = occurrences.size();
                occurrences.push_back({node, ring, previous, kAbsent});
                nodes[node].occurrences.push_back(occurrence);
                if (previous != kAbsent)
                    occurrences[previous].next = occurrence;
                else
                    first = occurrence;
                previous = occurrence;
                last_node = node;
            }
            if (first == kAbsent) invalid("ST_CoverageSimplify empty polygon ring");
            // The consecutive run can cross the circular seam. Remove its
            // trailing occurrence before linking the ring, without changing
            // the original WKB or the admitted coordinate/byte counts.
            if (previous != first && last_node == first_node) {
                const uint32_t trailing = previous;
                previous = occurrences[trailing].previous;
                nodes[first_node].occurrences.pop_back();
                occurrences.pop_back();
            }
            occurrences[first].previous = previous;
            occurrences[previous].next = first;
            rings.push_back({first, uint32_t(occurrences.size() - first), component, r != 0, Integer(0)});
            if (rings.back().count < 3) invalid("ST_CoverageSimplify degenerate polygon ring");
            uint32_t o = first;
            do {
                tick();
                const auto& a = nodes[occurrences[o].node].point;
                const auto& b = nodes[occurrences[occurrences[o].next].node].point;
                rings.back().area += a.ex * b.ey - a.ey * b.ex;
                o = occurrences[o].next;
            } while (o != first);
            if (rings.back().area == 0) invalid("ST_CoverageSimplify zero-area polygon ring");
        }
    }
    void decode() {
        for (uint32_t r = 0; r < rows.size(); ++r) {
            check();
            auto& row = rows[r];
            row.first = components.size();
            if (row.null) continue;
            Reader reader(Slice(reinterpret_cast<const char*>(row.bytes.data()), row.bytes.size()));
            auto [little, type] = reader.header();
            row.multi = type == 6;
            if (!row.multi)
                polygon(reader, r, little);
            else {
                uint32_t n = reader.number<uint32_t>(little);
                for (uint32_t i = 0; i < n; ++i) {
                    auto [order, child] = reader.header();
                    polygon(reader, r, order);
                }
            }
            row.count = components.size() - row.first;
            reader.done();
        }
        for (uint32_t n = 0; n < nodes.size(); ++n) {
            tick();
            node_tree.insert({bounds(nodes[n].point, nodes[n].point), n});
        }
    }
    void add_edge(uint32_t occurrence) {
        tick();
        uint32_t a = occurrences[occurrence].node, b = occurrences[occurrences[occurrence].next].node;
        auto [it, fresh] = edge_map.emplace(key(a, b), edges.size());
        if (fresh) {
            edges.push_back({std::min(a, b), std::max(a, b)});
            edge_tree.insert({bounds(nodes[a].point, nodes[b].point), it->second});
        }
        auto& edge = edges[it->second];
        if (edge.count == 2) invalid("ST_CoverageSimplify more than two coverage edge owners");
        edge.owners[edge.count++] = occurrence;
    }
    void erase_edge(uint32_t id) {
        auto& edge = edges[id];
        // Boost 1.80 remove() owns an uncustomizable underflow std::vector.
        // Keep old indexed items, mark inactive and filter queries. At most
        // one new edge per removed node, hence bounded storage, without that
        // default allocator bypass. The charged traversal includes stale items.
        edge_map.erase(key(edge.a, edge.b));
        edge.active = false;
    }
    void graph() {
        for (uint32_t o = 0; o < occurrences.size(); ++o) add_edge(o);
        std::pmr::vector<unsigned> degree(nodes.size(), 0, &memory);
        for (const auto& edge : edges) {
            tick();
            ++degree[edge.a];
            ++degree[edge.b];
        }
        for (size_t i = 0; i < nodes.size(); ++i) {
            tick();
            nodes[i].fixed = degree[i] != 2;
        }
    }
    // Exact doubled edge-midpoint classification; no rounded sample or epsilon.
    int location(const Integer& x, const Integer& y, const Ring& ring) {
        bool inside = false;
        uint32_t o = ring.first;
        do {
            tick();
            const auto& a = nodes[occurrences[o].node].point;
            const auto& b = nodes[occurrences[occurrences[o].next].node].point;
            Integer side = (b.ex - a.ex) * (y - 2 * a.ey) - (b.ey - a.ey) * (x - 2 * a.ex);
            if (side == 0 && 2 * std::min(a.ex, b.ex) <= x && x <= 2 * std::max(a.ex, b.ex) &&
                2 * std::min(a.ey, b.ey) <= y && y <= 2 * std::max(a.ey, b.ey))
                return 0;
            if ((2 * a.ey > y) != (2 * b.ey > y) && sign(side) == sign(b.ey - a.ey)) inside = !inside;
            o = occurrences[o].next;
        } while (o != ring.first);
        return inside ? 1 : -1;
    }
    int location(const Integer& x, const Integer& y, const Component& component) {
        if (component.count == 0) return -1;
        int outer = location(x, y, rings[component.first]);
        if (outer != 1) return outer;
        for (uint32_t r = 1; r < component.count; ++r) {
            int hole = location(x, y, rings[component.first + r]);
            if (hole >= 0) return -hole;
        }
        return 1;
    }
    template <class Region>
    int relation(const Ring& ring, const Region& region) {
        int relation = 0;
        uint32_t o = ring.first;
        do {
            const auto& a = nodes[occurrences[o].node].point;
            const auto& b = nodes[occurrences[occurrences[o].next].node].point;
            int next = location(a.ex + b.ex, a.ey + b.ey, region);
            if (next != 0) {
                if (relation != 0 && relation != next) invalid("ST_CoverageSimplify crossing polygon interiors");
                relation = next;
            }
            o = occurrences[o].next;
        } while (o != ring.first);
        return relation;
    }
    int side(uint32_t owner, const Edge& edge) const {
        const auto& occurrence = occurrences[owner];
        const auto& ring = rings[occurrence.ring];
        return sign(ring.area) * (ring.hole ? -1 : 1) * (occurrence.node == edge.a ? 1 : -1);
    }
    void validate() {
        check();
        // Contact incidence forest detects disconnected interiors (e.g. a hole
        // touching its shell twice) without a quadratic ring-pair allocation.
        std::pmr::vector<uint32_t> parent(rings.size(), &memory);
        std::iota(parent.begin(), parent.end(), 0);
        std::pmr::map<Key, uint32_t> contacts(&memory);
        std::pmr::map<Key, bool> links(&memory);
        auto root = [&](uint32_t id) {
            while (parent[id] != id) {
                tick();
                parent[id] = parent[parent[id]];
                id = parent[id];
            }
            return id;
        };
        auto link = [&](uint32_t ring, uint32_t node) {
            uint32_t component = rings[ring].component;
            auto [it, fresh] = contacts.emplace(Key{component, node}, parent.size());
            if (fresh) parent.push_back(parent.size());
            if (!links.emplace(Key{ring, it->second}, true).second) return;
            uint32_t a = root(ring), b = root(it->second);
            if (a == b) invalid("ST_CoverageSimplify disconnected polygon interior");
            parent[a] = b;
        };
        for (uint32_t i = 0; i < edges.size(); ++i) {
            auto& edge = edges[i];
            if (!edge.active) continue;
            tick();
            if (edge.count == 2) {
                const auto& a = rings[occurrences[edge.owners[0]].ring];
                const auto& b = rings[occurrences[edge.owners[1]].ring];
                if (a.component == b.component || components[a.component].row == components[b.component].row ||
                    side(edge.owners[0], edge) == side(edge.owners[1], edge))
                    invalid("ST_CoverageSimplify overlapping polygon boundary owners");
            }
            tick(edge_tree.size()); // Conservative bound includes index traversal.
            query(edge_tree, bounds(nodes[edge.a].point, nodes[edge.b].point), [&](const Item& item) {
                uint32_t j = item.second;
                if (j <= i || !edges[j].active) return;
                const auto& other = edges[j];
                tick();
                Contact c =
                        contact(nodes[edge.a].point, nodes[edge.b].point, nodes[other.a].point, nodes[other.b].point);
                if (c == Contact::NONE) return;
                if (c != Contact::ENDPOINT)
                    invalid("ST_CoverageSimplify invalid coverage crossing or mismatched subdivision");
                uint32_t node = (edge.a == other.a || edge.a == other.b) ? edge.a : edge.b;
                for (unsigned a = 0; a < edge.count; ++a)
                    for (unsigned b = 0; b < other.count; ++b) {
                        uint32_t x = edge.owners[a], y = other.owners[b];
                        const auto& ox = occurrences[x];
                        const auto& oy = occurrences[y];
                        if (ox.ring == oy.ring) {
                            if (ox.next != y && oy.next != x) invalid("ST_CoverageSimplify self-touching polygon ring");
                        } else if (rings[ox.ring].component == rings[oy.ring].component) {
                            link(ox.ring, node);
                            link(oy.ring, node);
                        }
                    }
            });
        }
        Tree polygons{bgi::quadratic<16>(), bgi::indexable<Item>(), bgi::equal_to<Item>(), Alloc<Item>(&memory)};
        for (uint32_t c = 0; c < components.size(); ++c) {
            const auto& component = components[c];
            if (component.count == 0) continue;
            const auto& outer = rings[component.first];
            for (uint32_t h = 1; h < component.count; ++h) {
                const auto& hole = rings[component.first + h];
                if (relation(hole, outer) != 1 || relation(outer, hole) >= 0)
                    invalid("ST_CoverageSimplify hole outside its shell");
                for (uint32_t k = 1; k < h; ++k)
                    if (relation(hole, rings[component.first + k]) >= 0 ||
                        relation(rings[component.first + k], hole) >= 0)
                        invalid("ST_CoverageSimplify overlapping holes");
            }
            uint32_t o = outer.first;
            const auto& first = nodes[occurrences[o].node].point;
            long double xmin = first.x, xmax = first.x, ymin = first.y, ymax = first.y;
            do {
                tick();
                const auto& p = nodes[occurrences[o].node].point;
                xmin = std::min(xmin, (long double)p.x);
                xmax = std::max(xmax, (long double)p.x);
                ymin = std::min(ymin, (long double)p.y);
                ymax = std::max(ymax, (long double)p.y);
                o = occurrences[o].next;
            } while (o != outer.first);
            Box box{{xmin, ymin}, {xmax, ymax}};
            tick(components.size());
            query(polygons, box, [&](const Item& item) {
                const auto& other = components[item.second];
                for (uint32_t r = 0; r < component.count; ++r)
                    if (relation(rings[component.first + r], other) > 0)
                        invalid("ST_CoverageSimplify overlapping coverage interiors");
                for (uint32_t r = 0; r < other.count; ++r)
                    if (relation(rings[other.first + r], component) > 0)
                        invalid("ST_CoverageSimplify overlapping coverage interiors");
            });
            polygons.insert({box, c});
        }
        check();
    }
    bool candidate(uint32_t n, bool boundary, uint32_t& a, uint32_t& c) {
        auto& node = nodes[n];
        if (node.fixed || !node.active || node.occurrences.empty()) return false;
        bool first = true;
        for (uint32_t o : node.occurrences) {
            const auto& occurrence = occurrences[o];
            if (!occurrence.active || rings[occurrence.ring].count <= 3) return false;
            const uint32_t left = occurrences[occurrence.previous].node, right = occurrences[occurrence.next].node;
            if (first) {
                a = std::min(left, right);
                c = std::max(left, right);
                first = false;
            } else if (key(left, right) != Key{a, c})
                return false;
        }
        if (a == c) return false;
        auto left = edge_map.find(key(a, n)), right = edge_map.find(key(n, c));
        if (left == edge_map.end() || right == edge_map.end()) return false;
        if (!boundary && (edges[left->second].count != 2 || edges[right->second].count != 2)) return false;
        return true;
    }
    void enqueue(uint32_t n, bool boundary, const Integer& floor) {
        uint32_t a, c;
        if (!candidate(n, boundary, a, c)) return;
        Integer area = boost::multiprecision::abs(orientation(nodes[a].point, nodes[n].point, nodes[c].point));
        if (area < floor) area = floor;
        heap.push_back({std::move(area), n, nodes[n].version});
        std::push_heap(heap.begin(), heap.end(), CandidateLess{});
    }
    bool removable(uint32_t n, uint32_t a, uint32_t c) {
        const auto& p = nodes[a].point;
        const auto& q = nodes[n].point;
        const auto& r = nodes[c].point;
        uint32_t left = edge_map.at(key(a, n)), right = edge_map.at(key(n, c));
        tick(edge_tree.size());
        bool safe = true;
        query(edge_tree, bounds(p, r), [&](const Item& item) {
            uint32_t id = item.second;
            if (id == left || id == right || !edges[id].active || !safe) return;
            tick();
            const auto& e = edges[id];
            auto kind = contact(p, r, nodes[e.a].point, nodes[e.b].point);
            if (kind != Contact::NONE && kind != Contact::ENDPOINT) safe = false;
            if (kind == Contact::ENDPOINT && e.a != a && e.b != a && e.a != c && e.b != c) safe = false;
        });
        if (!safe) return false;
        Integer area = orientation(p, q, r);
        if (area != 0) {
            Box box{{std::min({p.x, q.x, r.x}), std::min({p.y, q.y, r.y})},
                    {std::max({p.x, q.x, r.x}), std::max({p.y, q.y, r.y})}};
            tick(nodes.size());
            query(node_tree, box, [&](const Item& item) {
                uint32_t id = item.second;
                if (id == a || id == n || id == c || !nodes[id].active || !safe) return;
                tick();
                const auto& point = nodes[id].point;
                int x = sign(orientation(p, q, point)), y = sign(orientation(q, r, point)),
                    z = sign(orientation(r, p, point));
                if ((x >= 0 && y >= 0 && z >= 0) || (x <= 0 && y <= 0 && z <= 0)) safe = false;
            });
            if (!safe) return false;
        }
        for (uint32_t o : nodes[n].occurrences) {
            const auto& occurrence = occurrences[o];
            const auto& ring = rings[occurrence.ring];
            Integer next = ring.area - orientation(nodes[occurrences[occurrence.previous].node].point, q,
                                                   nodes[occurrences[occurrence.next].node].point);
            if (sign(next) != sign(ring.area)) return false;
        }
        return true;
    }
    void remove(uint32_t n, uint32_t a, uint32_t c) {
        erase_edge(edge_map.at(key(a, n)));
        erase_edge(edge_map.at(key(n, c)));
        for (uint32_t o : nodes[n].occurrences) {
            auto& occurrence = occurrences[o];
            auto& ring = rings[occurrence.ring];
            auto& previous = occurrences[occurrence.previous];
            auto& next = occurrences[occurrence.next];
            ring.area -= orientation(nodes[previous.node].point, nodes[n].point, nodes[next.node].point);
            previous.next = occurrence.next;
            next.previous = occurrence.previous;
            if (ring.first == o) ring.first = occurrence.next;
            --ring.count;
            occurrence.active = false;
            add_edge(occurrence.previous);
        }
        nodes[n].active = false;
        ++nodes[a].version;
        ++nodes[c].version;
    }
    void simplify(double tolerance, bool boundary) {
        ++calls;
        const Integer t = scaled(tolerance), threshold = 2 * t * t;
        Integer floor = 0;
        for (uint32_t n = 0; n < nodes.size(); ++n) {
            tick();
            enqueue(n, boundary, floor);
        }
        while (!heap.empty()) {
            tick();
            std::pop_heap(heap.begin(), heap.end(), CandidateLess{});
            Candidate next = std::move(heap.back());
            heap.pop_back();
            if (next.area > threshold) break;
            if (!nodes[next.node].active || next.version != nodes[next.node].version) continue;
            uint32_t a, c;
            if (!candidate(next.node, boundary, a, c) || !removable(next.node, a, c)) continue;
            floor = std::max(floor, next.area);
            remove(next.node, a, c);
            enqueue(a, boundary, floor);
            enqueue(c, boundary, floor);
        }
        validate();
    }
    void emit_polygon(std::pmr::vector<uint8_t>& output, const Component& component) {
        write<uint8_t>(output, 1);
        write<uint32_t>(output, 3);
        write<uint32_t>(output, component.count);
        for (uint32_t r = 0; r < component.count; ++r) {
            const auto& ring = rings[component.first + r];
            write<uint32_t>(output, ring.count + 1);
            uint32_t o = ring.first;
            do {
                tick();
                const auto& p = nodes[occurrences[o].node].point;
                write<double>(output, p.x);
                write<double>(output, p.y);
                o = occurrences[o].next;
            } while (o != ring.first);
            const auto& p = nodes[occurrences[ring.first].node].point;
            write<double>(output, p.x);
            write<double>(output, p.y);
        }
    }
    void output(bool unchanged) {
        // Removing source vertices cannot enlarge native WKB. One bounded
        // allocation gives the window adapter contiguous read-only backing.
        output_bytes.reserve(input_bytes);
        for (auto& row : rows) {
            check();
            row.output_first = output_bytes.size();
            if (row.null) continue;
            if (unchanged) {
                output_bytes.insert(output_bytes.end(), row.bytes.begin(), row.bytes.end());
            } else {
                if (row.multi) {
                    write<uint8_t>(output_bytes, 1);
                    write<uint32_t>(output_bytes, 6);
                    write<uint32_t>(output_bytes, row.count);
                }
                for (uint32_t c = 0; c < row.count; ++c) emit_polygon(output_bytes, components[row.first + c]);
            }
            row.output_size = output_bytes.size() - row.output_first;
        }
        check();
    }
};

StatusOr<std::unique_ptr<GeoCoverageSimplify>> GeoCoverageSimplify::create(memory::Allocator* allocator,
                                                                           GeoCoverageLimits limits,
                                                                           GeoCoverageCheckpoint checkpoint) {
    if (!allocator || !limits.rows || !limits.vertices || !limits.input_bytes || !limits.working_bytes || !limits.work)
        return Status::InvalidArgument("ST_CoverageSimplify requires positive coverage limits and an allocator");
    const size_t request = sizeof(StateAllocation) + sizeof(GeoCoverageSimplify) + sizeof(Impl) +
                           alignof(StateAllocation) + alignof(Impl);
    const int64_t metadata = allocator->nallox(request);
    if (metadata <= 0 || uint64_t(metadata) > limits.working_bytes)
        return Status::MemoryLimitExceeded("ST_CoverageSimplify exceeds geo_coverage_max_working_bytes_per_partition");
    void* raw = nullptr;
    Impl* impl = nullptr;
    try {
        raw = allocator->alloc(request);
        if (!raw) throw std::bad_alloc();
        void* public_location = static_cast<char*>(raw) + sizeof(StateAllocation);
        size_t space = request - sizeof(StateAllocation);
        std::align(alignof(StateAllocation), sizeof(GeoCoverageSimplify), public_location, space);
        new (static_cast<char*>(public_location) - sizeof(StateAllocation)) StateAllocation{allocator, raw, request};
        void* impl_location = static_cast<char*>(public_location) + sizeof(GeoCoverageSimplify);
        space = request - (static_cast<char*>(impl_location) - static_cast<char*>(raw));
        std::align(alignof(Impl), sizeof(Impl), impl_location, space);
        impl = new (impl_location) Impl(allocator, limits, checkpoint, metadata);
        impl->check();
        auto* result = new (public_location) GeoCoverageSimplify(impl);
        return std::unique_ptr<GeoCoverageSimplify>(result);
    } catch (const Stop& stop) {
        if (impl) impl->~Impl();
        if (raw) allocator->free(raw, request);
        return stop.status;
    } catch (const std::bad_alloc&) {
        if (impl) impl->~Impl();
        if (raw) allocator->free(raw, request);
        return Status::MemoryLimitExceeded("ST_CoverageSimplify state allocation failed");
    }
}
GeoCoverageSimplify::~GeoCoverageSimplify() {
    _impl->~Impl();
}
void GeoCoverageSimplify::operator delete(void* pointer) noexcept {
    auto* header = reinterpret_cast<StateAllocation*>(static_cast<char*>(pointer) - sizeof(StateAllocation));
    auto allocation = *header;
    header->~StateAllocation();
    allocation.allocator->free(allocation.raw, allocation.bytes);
}
Status GeoCoverageSimplify::append(std::optional<Slice> wkb) {
    auto& s = *_impl;
    if (s.finished || s.failed) return Status::InvalidArgument("ST_CoverageSimplify partition is closed");
    try {
        MemoryScope scope(s.memory);
        s.check();
        if (s.rows.size() == s.limits.rows) invalid("ST_CoverageSimplify exceeds geo_coverage_max_rows_per_partition");
        size_t count = s.vertices;
        if (wkb) {
            if (wkb->size > s.limits.input_bytes - s.input_bytes)
                invalid("ST_CoverageSimplify exceeds geo_coverage_max_input_bytes_per_partition");
            Reader reader(*wkb);
            s.preflight(reader, false, count);
            reader.done();
        }
        s.rows.emplace_back(s.memory);
        auto& row = s.rows.back();
        row.null = !wkb;
        if (wkb) {
            row.bytes.assign(reinterpret_cast<const uint8_t*>(wkb->data),
                             reinterpret_cast<const uint8_t*>(wkb->data) + wkb->size);
            s.input_bytes += wkb->size;
            s.vertices = count;
        }
        return Status::OK();
    } catch (const Stop& stop) {
        s.failed = true;
        return stop.status;
    } catch (const std::bad_alloc&) {
        s.failed = true;
        return Status::MemoryLimitExceeded("ST_CoverageSimplify admission allocation failed");
    } catch (const std::exception& e) {
        s.failed = true;
        return Status::InvalidArgument(std::string("ST_CoverageSimplify admission failed: ") + e.what());
    }
}
Status GeoCoverageSimplify::finish(std::optional<double> tolerance, std::optional<bool> boundary) {
    auto& s = *_impl;
    if (s.finished || s.failed) return Status::InvalidArgument("ST_CoverageSimplify partition is closed");
    try {
        MemoryScope scope(s.memory);
        s.check();
        s.null_parameter = !tolerance || !boundary;
        if (!s.null_parameter) {
            if (!std::isfinite(*tolerance) || *tolerance < 0 || !std::isfinite(*tolerance * *tolerance))
                invalid("ST_CoverageSimplify requires finite nonnegative tolerance and representable tolerance "
                        "squared");
            s.decode();
            s.graph();
            s.validate();
            if (*tolerance > 0 && !s.nodes.empty()) s.simplify(*tolerance, *boundary);
            s.output(*tolerance == 0);
        }
        s.finished = true;
        return Status::OK();
    } catch (const Stop& stop) {
        s.failed = true;
        return stop.status;
    } catch (const std::bad_alloc&) {
        s.failed = true;
        return Status::MemoryLimitExceeded("ST_CoverageSimplify kernel allocation failed");
    } catch (const std::exception& e) {
        s.failed = true;
        return Status::InvalidArgument(std::string("ST_CoverageSimplify kernel failed: ") + e.what());
    }
}
StatusOr<std::optional<Slice>> GeoCoverageSimplify::result(size_t position) const {
    const auto& s = *_impl;
    if (!s.finished || s.failed || position >= s.rows.size())
        return Status::InvalidArgument("ST_CoverageSimplify result unavailable");
    const auto& row = s.rows[position];
    if (s.null_parameter || row.null) return std::optional<Slice>{};
    return std::optional<Slice>{
            Slice(reinterpret_cast<const char*>(s.output_bytes.data()) + row.output_first, row.output_size)};
}
StatusOr<Slice> GeoCoverageSimplify::result_buffer() const {
    if (!_impl->finished || _impl->failed) return Status::InvalidArgument("ST_CoverageSimplify result unavailable");
    return Slice(reinterpret_cast<const char*>(_impl->output_bytes.data()), _impl->output_bytes.size());
}
std::pmr::memory_resource* GeoCoverageSimplify::resource() {
    return &_impl->memory;
}
Status GeoCoverageSimplify::retain_external(size_t bytes) {
    try {
        _impl->memory.retain(bytes);
        _impl->external += bytes;
        return Status::OK();
    } catch (const Stop& stop) {
        _impl->failed = true;
        return stop.status;
    }
}
void GeoCoverageSimplify::release_external(size_t bytes) {
    const size_t release = std::min(bytes, _impl->external);
    _impl->external -= release;
    _impl->memory.release(release);
}
size_t GeoCoverageSimplify::rows() const {
    return _impl->rows.size();
}
size_t GeoCoverageSimplify::memory_usage() const {
    return _impl->memory.used;
}
size_t GeoCoverageSimplify::peak_memory_usage() const {
    return _impl->memory.peak;
}
size_t GeoCoverageSimplify::kernel_calls() const {
    return _impl->calls;
}
} // namespace starrocks
