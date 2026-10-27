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
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <span>
#include <variant>
#include <vector>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include "base/hash/hash_util.hpp"
#include "base/phmap/phmap.h"
#include "geo/geo_exact_arithmetic.h"
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
[[noreturn]] void invalid(const char* message) {
    throw Stop{Status::InvalidArgument(message)};
}

// uint32_t graph indices reserve the largest value for absent links.
uint32_t index(size_t size) {
    if (size >= kAbsent) invalid("ST_CoverageSimplify graph index range exceeded");
    return static_cast<uint32_t>(size);
}

// These maps are lookup-only: IDs are assigned on first encounter, never by
// hash iteration.
struct LookupHash {
    size_t operator()(const std::pair<double, double>& key) const {
        const std::array<uint64_t, 2> bits{key.first == 0 ? 0 : std::bit_cast<uint64_t>(key.first),
                                           key.second == 0 ? 0 : std::bit_cast<uint64_t>(key.second)};
        return HashUtil::murmur_hash64A(bits, 0);
    }
    size_t operator()(const Key& key) const {
        const uint64_t bits = uint64_t(key.first) << 32 | key.second;
        return HashUtil::murmur_hash64A(bits, 0);
    }
};
template <class K, class V>
using Map = phmap::flat_hash_map<K, V, LookupHash>;
// Finite binary64 scaled by 2^1074 has <2098 bits; determinants <4199,
// ring sums with native WKB positions <4219. 16384 checked bits also cover
// tolerance^2 and doubled midpoint samples. Dynamic limbs use the standard allocator.
using DynamicInteger = boost::multiprecision::number<
        boost::multiprecision::cpp_int_backend<128, 16384, boost::multiprecision::signed_magnitude,
                                               boost::multiprecision::checked,
                                               std::allocator<boost::multiprecision::limb_type>>,
        boost::multiprecision::et_off>;
using FixedInteger = boost::multiprecision::number<
        boost::multiprecision::cpp_int_backend<256, 256, boost::multiprecision::signed_magnitude,
                                               boost::multiprecision::checked, void>,
        boost::multiprecision::et_off>;

// Finite binary64 as an odd significand times 2^shift, in the original 2^1074 scale.
struct Binary64 {
    uint64_t significand;
    unsigned shift;
    bool negative;
    explicit Binary64(double value) {
        const uint64_t bits = std::bit_cast<uint64_t>(value), exponent = (bits >> 52) & 0x7ff;
        significand = bits & ((uint64_t{1} << 52) - 1);
        if (exponent) significand |= uint64_t{1} << 52;
        shift = exponent ? exponent - 1 : 0;
        negative = bits >> 63;
        if (significand) {
            const unsigned zeros = std::countr_zero(significand);
            significand >>= zeros;
            shift += zeros;
        }
    }
};
using NativeInteger = __int128;
using boost::multiprecision::abs;
NativeInteger abs(NativeInteger value) {
    return value < 0 ? -value : value;
}
unsigned bit_width128(unsigned __int128 value) {
    const auto upper = uint64_t(value >> 64);
    return upper ? 64 + std::bit_width(upper) : std::bit_width(uint64_t(value));
}
// Strict bit bound of the exact binary64 difference in the 2^1074 scale.
// Nearby endpoints use unsigned arithmetic; wider gaps use a conservative
// exponent bound. No rounded subtraction participates in fast-path admission.
unsigned difference_high_bit(double left, double right) {
    const Binary64 a(left), b(right);
    const unsigned ah = a.significand ? a.shift + std::bit_width(a.significand) : 0;
    const unsigned bh = b.significand ? b.shift + std::bit_width(b.significand) : 0;
    if (!ah || !bh) return std::max(ah, bh);
    const unsigned shift = std::min(a.shift, b.shift), high = std::max(ah, bh);
    if (high - shift > 127) return high + 1;
    const unsigned __int128 av = (unsigned __int128)(a.significand) << (a.shift - shift);
    const unsigned __int128 bv = (unsigned __int128)(b.significand) << (b.shift - shift);
    const unsigned __int128 delta = a.negative != b.negative ? av + bv : (av < bv ? bv - av : av - bv);
    return delta ? shift + bit_width128(delta) : 0;
}
struct ArithmeticBounds {
    unsigned common_shift = 2098, high_bit = 0, extent_high = 0;
    uint32_t positions = 0;
    void include(double value) {
        const Binary64 parts(value);
        if (!parts.significand) return;
        common_shift = std::min(common_shift, parts.shift);
        high_bit = std::max(high_bit, parts.shift + unsigned(std::bit_width(parts.significand)));
    }
    void include_ring(double xmin, double ymin, double xmax, double ymax) {
        extent_high = std::max({extent_high, difference_high_bit(xmin, xmax), difference_high_bit(ymin, ymax)});
    }
    bool native(double tolerance) const {
        const unsigned coordinate_bits = high_bit ? high_bit - common_shift : 0;
        const Binary64 t(tolerance);
        const unsigned tolerance_high = t.significand ? t.shift + std::bit_width(t.significand) : 0;
        const unsigned local_high = std::max(extent_high, tolerance_high);
        const unsigned b = local_high ? local_high - common_shift : 0;
        // Absolute coordinates/doubled midpoint sums fit signed128. Every edge
        // stays within an original ring bbox. Overlapping edge boxes span at
        // most twice the largest ring extent; contact determinants use <2b+3
        // bits. Ring-local area partials and removals use <2b+bit_width(N)+3.
        // Six guard bits also cover doubled midpoint determinants and 2*t*t.
        return coordinate_bits + 2 <= 127 && 2 * b + std::bit_width(positions) + 6 <= 127;
    }
    bool fixed() const {
        const unsigned b = high_bit ? high_bit - common_shift : 0;
        // |coordinate|, tolerance <2^b. Determinants use <=2b+4 bits;
        // every (also shortened) ring sum uses <=2b+1+bit_width(positions).
        // The first bound also keeps multiplication operands within two limbs,
        // so Boost's full four-limb multiplication scratch fits in 256 bits.
        return b + 2 <= 128 && 2 * b + std::bit_width(positions) + 4 <= 255;
    }
};
template <class Integer>
int sign(const Integer& value) {
    return geo_exact::sign(value);
}
template <class Integer>
struct NumericPoint {
    double x, y;
    Integer ex, ey;
};
template <class Integer>
Integer orientation(const NumericPoint<Integer>& a, const NumericPoint<Integer>& b, const NumericPoint<Integer>& c) {
    return geo_exact::orientation(a.ex, a.ey, b.ex, b.ey, c.ex, c.ey);
}
Key key(uint32_t a, uint32_t b) {
    return std::minmax(a, b);
}
// Wider exponent range prevents box split/area overflow at binary64 extremes.
// Index boxes only select candidates; all topology decisions are exact.
using BoxPoint = bg::model::d2::point_xy<long double>;
using Box = bg::model::box<BoxPoint>;
using Item = std::pair<Box, uint32_t>;
using Tree = bgi::rtree<Item, bgi::quadratic<16>>;
Tree insertion_tree(const std::vector<Item>& items) {
    Tree tree;
    for (const auto& item : items) tree.insert(item);
    return tree;
}

// query() traverses recursively with stack storage. qbegin() in Boost 1.80
// instead owns an uncustomizable std::vector path stack, so it is not used.
template <class F>
struct QueryOutput {
    F* visitor;
    QueryOutput& operator*() { return *this; }
    QueryOutput& operator++() { return *this; }
    QueryOutput operator++(int) { return *this; }
    QueryOutput& operator=(const Item& item) {
        (*visitor)(item.second);
        return *this;
    }
};
template <class F>
void query(const Tree& tree, const Box& box, F&& visitor) {
    tree.query(bgi::intersects(box), QueryOutput<std::remove_reference_t<F>>{&visitor});
}

// A cell scan is at most four 16-entry R-tree leaves; linear overflow is
// limited to one leaf. 64 cells per item leaves about 3x the observed query
// work on large rings, while rejecting sparse, wide-query partitions.
constexpr size_t kMaxGridCellEntries = 64;
constexpr size_t kMaxGridOverflow = 16;
constexpr size_t kGridQueryCellsPerItem = 64;
// Measured maxima are 6 cells on coverage_10k and 40 on the 2048-edge rings.
// A single wider query promotes the index, without a running work counter.
constexpr size_t kMaxGridQueryCells = 64;

template <class F>
void query_ordered(const Tree& tree, const Box& box, std::vector<uint32_t>& candidates, F&& visitor) {
    candidates.clear();
    query(tree, box, [&](uint32_t id) { candidates.push_back(id); });
    std::sort(candidates.begin(), candidates.end());
    for (uint32_t id : candidates) visitor(id);
}

struct CellRange {
    size_t x0, y0, x1, y1;
    size_t size() const { return (x1 - x0 + 1) * (y1 - y0 + 1); }
    bool short_box() const { return x1 - x0 <= 1 && y1 - y0 <= 1; }
};

// Rounded monotone cell assignment selects candidates only; topology stays exact.
// The span term bounds cell storage by O(N), including thin partitions.
struct Grid {
    double xmin = 0, ymin = 0, inverse = 0;
    size_t nx = 0, ny = 0, links = 0;
    std::vector<size_t> counts;
    static Grid layout(const std::vector<Item>& items, bool edges) {
        Grid result;
        if (items.empty()) return result;
        double xmin = std::numeric_limits<double>::infinity(), ymin = xmin;
        double xmax = -xmin, ymax = -xmin;
        std::vector<double> lengths;
        if (edges) lengths.reserve(items.size());
        for (const auto& item : items) {
            const double x0 = bg::get<bg::min_corner, 0>(item.first), y0 = bg::get<bg::min_corner, 1>(item.first);
            const double x1 = bg::get<bg::max_corner, 0>(item.first), y1 = bg::get<bg::max_corner, 1>(item.first);
            xmin = std::min(xmin, x0);
            ymin = std::min(ymin, y0);
            xmax = std::max(xmax, x1);
            ymax = std::max(ymax, y1);
            if (edges) lengths.push_back(std::max(x1 - x0, y1 - y0));
        }
        const double dx = xmax - xmin, dy = ymax - ymin;
        double width = std::max(dx, dy) / std::sqrt(double(items.size()));
        if (edges) {
            auto middle = lengths.begin() + lengths.size() / 2;
            std::nth_element(lengths.begin(), middle, lengths.end());
            width = std::max(width, *middle);
        }
        const double scale = 1 / width;
        if (std::isfinite(dx) && std::isfinite(dy) && width > 0 && std::isfinite(width) && scale > 0 &&
            std::isfinite(scale)) {
            result.xmin = xmin;
            result.ymin = ymin;
            result.inverse = scale;
            result.nx = size_t(dx * scale) + 1;
            result.ny = size_t(dy * scale) + 1;
        }
        return result;
    }
    CellRange range(double x0, double y0, double x1, double y1) const {
        auto cell = [&](double value, double origin, size_t count) {
            return size_t(std::clamp((value - origin) * inverse, 0.0, double(count - 1)));
        };
        return {cell(x0, xmin, nx), cell(y0, ymin, ny), cell(x1, xmin, nx), cell(y1, ymin, ny)};
    }
    bool fits(const std::vector<Item>& items, const std::vector<Item>& edges, size_t nodes) {
        if (!nx) return false;
        counts.assign(nx * ny + 1, 0);
        size_t overflow = 0;
        size_t remaining = kGridQueryCellsPerItem * items.size(), width = 1, height = 1;
        const auto cell_range = [&](const Item& item) {
            return range(bg::get<bg::min_corner, 0>(item.first), bg::get<bg::min_corner, 1>(item.first),
                         bg::get<bg::max_corner, 0>(item.first), bg::get<bg::max_corner, 1>(item.first));
        };
        const auto query_work = [&](const CellRange& r) {
            if (r.size() > remaining) return false;
            remaining -= r.size();
            width = std::max(width, r.x1 - r.x0 + 1);
            height = std::max(height, r.y1 - r.y0 + 1);
            return true;
        };
        for (const auto& item : items) {
            const auto r = cell_range(item);
            if (&items == &edges && !query_work(r)) return false;
            if (!r.short_box()) {
                if (++overflow > kMaxGridOverflow) return false;
            } else {
                links += r.size();
                for (size_t y = r.y0; y <= r.y1; ++y)
                    for (size_t x = r.x0; x <= r.x1; ++x)
                        if (++counts[y * nx + x + 1] > kMaxGridCellEntries) return false;
            }
        }
        if (&items != &edges)
            for (const auto& edge : edges)
                if (!query_work(cell_range(edge))) return false;
        // A first removal triangle spans two adjacent original edges. Their
        // cell spans bound each triangle by (2*width-1)*(2*height-1), without
        // another pass over the graph's node/occurrence adjacency.
        return (2 * width - 1) * (2 * height - 1) <= remaining / nodes;
    }
};

size_t grid_cell(double value, double origin, double inverse, size_t count) {
    return size_t(std::clamp((value - origin) * inverse, 0.0, double(count - 1)));
}

class EdgeGrid {
    static constexpr size_t absent = std::numeric_limits<size_t>::max();
    struct Entry {
        double xmin, ymin, xmax, ymax;
        std::array<size_t, 4> links{};
        size_t overflow = absent;
        uint64_t seen = 0;
        unsigned count = 0;
        bool active = true;
    };
    struct Link {
        size_t next, previous, cell;
        uint32_t edge;
    };
    Tree fallback;
    std::vector<Entry> entries;
    std::vector<Link> links;
    std::vector<size_t> heads;
    std::vector<uint32_t> overflow, candidates;
    size_t free = absent, nx = 0, ny = 0;
    double xmin = 0, ymin = 0, inverse = 0;
    uint64_t epoch = 0;
    bool overflow_indexed = false;
    size_t cell(double value, double origin, size_t count) const { return grid_cell(value, origin, inverse, count); }
    static Entry entry(const Box& box) {
        return {double(bg::get<bg::min_corner, 0>(box)), double(bg::get<bg::min_corner, 1>(box)),
                double(bg::get<bg::max_corner, 0>(box)), double(bg::get<bg::max_corner, 1>(box))};
    }
    static Box box(const Entry& e) { return {{e.xmin, e.ymin}, {e.xmax, e.ymax}}; }
    void append(const Item& item) {
        Entry e = entry(item.first);
        const size_t x0 = cell(e.xmin, xmin, nx), x1 = cell(e.xmax, xmin, nx);
        const size_t y0 = cell(e.ymin, ymin, ny), y1 = cell(e.ymax, ymin, ny);
        if (x1 - x0 > 1 || y1 - y0 > 1) {
            e.overflow = overflow.size();
            overflow.push_back(item.second);
            if (overflow_indexed) fallback.insert(item);
        } else {
            for (size_t y = y0; y <= y1; ++y) {
                for (size_t x = x0; x <= x1; ++x) {
                    const size_t c = y * nx + x;
                    size_t id = free;
                    if (id == absent) {
                        id = links.size();
                        links.push_back({});
                    } else {
                        free = links[id].next;
                    }
                    links[id] = {heads[c], absent, c, item.second};
                    if (heads[c] != absent) links[heads[c]].previous = id;
                    heads[c] = id;
                    e.links[e.count++] = id;
                }
            }
        }
        entries.push_back(e);
    }

    void index_overflow() {
        std::vector<Item> items;
        items.reserve(overflow.size());
        for (uint32_t id : overflow) {
            items.push_back({box(entries[id]), id});
        }
        fallback = Tree(items.begin(), items.end());
        overflow_indexed = true;
    }

public:
    void build(const std::vector<Item>& items, Grid layout, size_t nodes) {
        xmin = layout.xmin;
        ymin = layout.ymin;
        inverse = layout.inverse;
        nx = layout.nx;
        ny = layout.ny;
        if (!nx) {
            fallback = insertion_tree(items);
            return;
        }
        heads = std::move(layout.counts);
        heads.pop_back();
        std::fill(heads.begin(), heads.end(), absent);
        // Each removed node creates at most one fresh chord. Admission also
        // counted the initial links, so neither array needs a guessed capacity.
        entries.reserve(items.size() + nodes);
        links.reserve(layout.links);
        for (const auto& item : items) append(item);
    }
    bool is_grid() const { return nx != 0; }
    void use_tree() {
        std::vector<Item> items;
        items.reserve(entries.size());
        for (uint32_t i = 0; i < entries.size(); ++i)
            if (entries[i].active) items.push_back({box(entries[i]), i});
        fallback = insertion_tree(items);
        nx = ny = 0;
        entries.clear();
        links.clear();
        heads.clear();
        overflow.clear();
    }
    void erase_tree(const Item& item) { fallback.remove(item); }
    void insert(const Item& item) {
        if (nx) {
            append(item);
            // A runtime promotion copies at most 17 entries.
            if (!overflow_indexed && overflow.size() > kMaxGridOverflow) index_overflow();
        } else {
            fallback.insert(item);
        }
    }
    void erase(uint32_t id) {
        auto& e = entries[id];
        for (unsigned i = 0; i < e.count; ++i) {
            const size_t id = e.links[i];
            auto& link = links[id];
            if (link.previous == absent)
                heads[link.cell] = link.next;
            else
                links[link.previous].next = link.next;
            if (link.next != absent) links[link.next].previous = link.previous;
            link.next = free;
            free = id;
        }
        if (e.overflow != absent) {
            // The grid filters stale overflow entries; full-tree mode removes them.
            const uint32_t last = overflow.back();
            overflow[e.overflow] = last;
            entries[last].overflow = e.overflow;
            overflow.pop_back();
        }
        e.active = false;
    }
    template <class F>
    void visit(const Box& query_box, F&& visitor) {
        if (!nx) {
            query_ordered(fallback, query_box, candidates, visitor);
            return;
        }
        const Entry q = entry(query_box);
        // Queries are bounded by graph/heap indices, far below 2^64 per run.
        ++epoch;
        candidates.clear();
        auto collect = [&](uint32_t id) {
            auto& e = entries[id];
            if (!e.active || e.seen == epoch) return;
            e.seen = epoch;
            if (e.xmin <= q.xmax && q.xmin <= e.xmax && e.ymin <= q.ymax && q.ymin <= e.ymax) candidates.push_back(id);
        };
        const size_t x0 = cell(q.xmin, xmin, nx), x1 = cell(q.xmax, xmin, nx);
        const size_t y0 = cell(q.ymin, ymin, ny), y1 = cell(q.ymax, ymin, ny);
        if ((x1 - x0 + 1) * (y1 - y0 + 1) > kMaxGridQueryCells) {
            use_tree();
            query_ordered(fallback, query_box, candidates, visitor);
            return;
        }
        for (size_t y = y0; y <= y1; ++y)
            for (size_t x = x0; x <= x1; ++x)
                for (size_t id = heads[y * nx + x]; id != absent; id = links[id].next) collect(links[id].edge);
        if (overflow_indexed)
            query(fallback, query_box, collect);
        else
            for (uint32_t id : overflow) collect(id);
        std::sort(candidates.begin(), candidates.end());
        for (uint32_t id : candidates) visitor(id);
    }
};
template <class F>
void query(EdgeGrid& grid, const Box& box, F&& visitor) {
    grid.visit(box, std::forward<F>(visitor));
}

// Point cells are immutable; the kernel filters inactive nodes. Promotion
// copies only active nodes, and subsequent removals update the R-tree.
class PointGrid {
    struct Point {
        double x, y;
        uint32_t id;
    };
    Tree fallback;
    std::vector<Point> points;
    std::vector<size_t> offsets;
    double xmin = 0, ymin = 0, inverse = 0;
    size_t nx = 0, ny = 0;
    size_t xcell(double x) const { return grid_cell(x, xmin, inverse, nx); }
    size_t ycell(double y) const { return grid_cell(y, ymin, inverse, ny); }

public:
    void build(const std::vector<Item>& items, Grid layout) {
        xmin = layout.xmin;
        ymin = layout.ymin;
        inverse = layout.inverse;
        nx = layout.nx;
        ny = layout.ny;
        if (!nx) {
            fallback = insertion_tree(items);
            return;
        }
        offsets = std::move(layout.counts);
        std::partial_sum(offsets.begin(), offsets.end(), offsets.begin());
        std::vector<size_t> next(offsets);
        points.resize(items.size());
        for (const auto& item : items) {
            const double x = bg::get<bg::min_corner, 0>(item.first), y = bg::get<bg::min_corner, 1>(item.first);
            points[next[ycell(y) * nx + xcell(x)]++] = {x, y, item.second};
        }
    }
    template <class Active>
    void use_tree(Active&& active) {
        std::vector<Item> items;
        items.reserve(points.size());
        for (const auto& p : points)
            if (active(p.id)) items.push_back({{{p.x, p.y}, {p.x, p.y}}, p.id});
        fallback = insertion_tree(items);
        nx = ny = 0;
        points.clear();
        offsets.clear();
    }
    void erase(uint32_t id, double x, double y) {
        if (!nx) fallback.remove({{{x, y}, {x, y}}, id});
    }
    template <class F, class Active>
    void visit(const Box& box, F&& visitor, Active&& active) {
        if (!nx) {
            query(fallback, box, visitor);
            return;
        }
        const double x0 = bg::get<bg::min_corner, 0>(box), y0 = bg::get<bg::min_corner, 1>(box);
        const double x1 = bg::get<bg::max_corner, 0>(box), y1 = bg::get<bg::max_corner, 1>(box);
        const size_t cx0 = xcell(x0), cy0 = ycell(y0), cx1 = xcell(x1), cy1 = ycell(y1);
        if ((cx1 - cx0 + 1) * (cy1 - cy0 + 1) > kMaxGridQueryCells) {
            use_tree(active);
            query(fallback, box, visitor);
            return;
        }
        for (size_t y = cy0; y <= cy1; ++y)
            for (size_t x = cx0; x <= cx1; ++x) {
                const size_t cell = y * nx + x;
                for (size_t i = offsets[cell]; i < offsets[cell + 1]; ++i) {
                    const auto& p = points[i];
                    if (x0 <= p.x && p.x <= x1 && y0 <= p.y && p.y <= y1) visitor(p.id);
                }
            }
    }
};

template <class Integer>
Box bounds(const NumericPoint<Integer>& a, const NumericPoint<Integer>& b) {
    return {{std::min(a.x, b.x), std::min(a.y, b.y)}, {std::max(a.x, b.x), std::max(a.y, b.y)}};
}
template <class Integer>
bool between(const NumericPoint<Integer>& p, const NumericPoint<Integer>& a, const NumericPoint<Integer>& b) {
    return std::min(a.x, b.x) <= p.x && p.x <= std::max(a.x, b.x) && std::min(a.y, b.y) <= p.y &&
           p.y <= std::max(a.y, b.y);
}
enum class Contact { NONE, ENDPOINT, TJUNCTION, CROSS, OVERLAP };
template <class Integer>
Contact contact(const NumericPoint<Integer>& a, const NumericPoint<Integer>& b, const NumericPoint<Integer>& c,
                const NumericPoint<Integer>& d) {
    auto equal = [](const NumericPoint<Integer>& p, const NumericPoint<Integer>& q) {
        return p.x == q.x && p.y == q.y;
    };
    int ac = 0, ad = 0, ca = 0, cb = 0;
    const bool shared_c = equal(a, c) || equal(b, c);
    if (shared_c || equal(a, d) || equal(b, d)) {
        // With a shared endpoint, a non-collinear pair meets only there.
        // Collinear pairs still use the original closed-interval test below.
        if (orientation(a, b, shared_c ? d : c) != 0) return Contact::ENDPOINT;
    } else {
        ac = sign(orientation(a, b, c));
        ad = sign(orientation(a, b, d));
        ca = sign(orientation(c, d, a));
        cb = sign(orientation(c, d, b));
    }
    if (ac * ad < 0 && ca * cb < 0) return Contact::CROSS;
    if (ac == 0 && ad == 0 && ca == 0 && cb == 0) {
        auto less = [](const NumericPoint<Integer>& p, const NumericPoint<Integer>& q) {
            return p.x < q.x || (p.x == q.x && p.y < q.y);
        };
        const auto& lo1 = less(a, b) ? a : b;
        const auto& hi1 = less(a, b) ? b : a;
        const auto& lo2 = less(c, d) ? c : d;
        const auto& hi2 = less(c, d) ? d : c;
        const auto& lo = less(lo1, lo2) ? lo2 : lo1;
        const auto& hi = less(hi1, hi2) ? hi1 : hi2;
        if (less(hi, lo)) return Contact::NONE;
        if (less(lo, hi)) return Contact::OVERLAP;
    }

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
template <class Integer>
struct NumericRing {
    uint32_t first, count;
    Integer area;
};
struct Component {
    uint32_t first, count;
};
template <class Integer>
struct NumericNode {
    NumericPoint<Integer> point;
    uint32_t first = 0, count = 0, version = 0;
    bool fixed = false, active = true;
    explicit NumericNode(NumericPoint<Integer> p) : point(std::move(p)) {}
};
struct Edge {
    uint32_t a, b;
    std::array<uint32_t, 2> owners{};
    unsigned count = 0;
    bool active = true;
};
struct Row {
    std::vector<uint8_t> bytes;
    size_t output_first = 0, output_size = 0;
    uint32_t first = 0, count = 0;
    bool null = false, multi = false;
};
template <class Integer>
struct NumericCandidate {
    Integer area;
    uint32_t node, version;
};
template <class Integer>
struct NumericCandidateLess {
    bool operator()(const NumericCandidate<Integer>& a, const NumericCandidate<Integer>& b) const {
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
void write(std::vector<uint8_t>& output, T value) {
    if constexpr (std::endian::native != std::endian::little) {
        auto* raw = reinterpret_cast<uint8_t*>(&value);
        std::reverse(raw, raw + sizeof(T));
    }
    const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
    output.insert(output.end(), bytes, bytes + sizeof(T));
}
struct Partition {
    std::vector<Row> rows;
    std::vector<uint8_t> output_bytes;
    size_t input_bytes = 0, calls = 0;
    bool finished = false, failed = false, null_parameter = false;
    ArithmeticBounds arithmetic;
    void preflight(Reader& reader, bool child) {
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
            if (type == 6) {
                preflight(reader, true);
                continue;
            }
            const uint32_t positions = reader.number<uint32_t>(little);
            if (positions > native_limits.max_coordinates - reader.coordinates)
                invalid("ST_CoverageSimplify exceeds native WKB coordinate limit");
            reader.coordinates += positions;
            arithmetic.positions = std::max(arithmetic.positions, positions);
            if (positions > (reader.bytes.size - reader.position) / 16)
                invalid("ST_CoverageSimplify truncated WKB ring");
            double xmin = std::numeric_limits<double>::infinity(), ymin = xmin;
            double xmax = -xmin, ymax = -xmin;
            for (uint32_t j = 0; j < positions; ++j) {
                double x = reader.number<double>(little), y = reader.number<double>(little);
                if (!std::isfinite(x) || !std::isfinite(y)) invalid("ST_CoverageSimplify nonfinite XY coordinate");
                arithmetic.include(x);
                arithmetic.include(y);
                xmin = std::min(xmin, x);
                ymin = std::min(ymin, y);
                xmax = std::max(xmax, x);
                ymax = std::max(ymax, y);
            }
            if (positions) arithmetic.include_ring(xmin, ymin, xmax, ymax);
        }
    }
};

template <class Integer>
struct Kernel {
    using Point = NumericPoint<Integer>;
    using Node = NumericNode<Integer>;
    using Ring = NumericRing<Integer>;
    using Candidate = NumericCandidate<Integer>;
    using CandidateLess = NumericCandidateLess<Integer>;
    std::vector<Row>& rows;
    std::vector<uint8_t>& output_bytes;
    size_t& input_bytes;
    size_t& calls;
    unsigned shift;
    std::vector<Node> nodes;
    std::vector<Occurrence> occurrences;
    std::vector<uint32_t> node_occurrence_ids;
    std::vector<Ring> rings;
    std::vector<Component> components;
    Map<std::pair<double, double>, uint32_t> node_map;
    Map<Key, uint32_t> edge_map;
    std::vector<Edge> edges;
    std::vector<Candidate> heap;
    EdgeGrid edge_tree;
    PointGrid node_tree;
    Kernel(Partition& partition, unsigned shift)
            : rows(partition.rows),
              output_bytes(partition.output_bytes),
              input_bytes(partition.input_bytes),
              calls(partition.calls),
              shift(shift) {}
    Integer scaled(double value) const {
        if constexpr (std::is_same_v<Integer, DynamicInteger>) {
            return geo_exact::scaled_binary64<Integer>(value);
        } else {
            const Binary64 parts(value);
            Integer result(parts.significand);
            if (parts.significand) result <<= parts.shift - shift;
            return parts.negative ? -result : result;
        }
    }
    void run(double tolerance, bool boundary) {
        decode();
        graph();
        if (tolerance > 0 && !nodes.empty()) simplify(tolerance, boundary);
        output(tolerance == 0);
    }
    uint32_t intern(double x, double y) {
        auto [it, fresh] = node_map.emplace(std::make_pair(x, y), 0);
        if (fresh) {
            it->second = index(nodes.size());
            nodes.emplace_back(Point{x, y, scaled(x), scaled(y)});
        }
        return it->second;
    }
    void polygon(Reader& reader, bool little) {
        uint32_t n = reader.number<uint32_t>(little);
        index(components.size());
        components.push_back({uint32_t(rings.size()), n});
        for (uint32_t r = 0; r < n; ++r) {
            const uint32_t positions = reader.number<uint32_t>(little), ring = index(rings.size());
            if (positions < 4) invalid("ST_CoverageSimplify degenerate polygon ring");
            uint32_t first = kAbsent, previous = kAbsent, first_node = kAbsent, last_node = kAbsent;
            for (uint32_t j = 0; j < positions; ++j) {
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
                uint32_t occurrence = index(occurrences.size());
                occurrences.push_back({node, ring, previous, kAbsent});
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
                occurrences.pop_back();
            }
            occurrences[first].previous = previous;
            occurrences[previous].next = first;
            rings.push_back({first, uint32_t(occurrences.size() - first), Integer(0)});
            if (rings.back().count < 3) invalid("ST_CoverageSimplify degenerate polygon ring");
            uint32_t o = first;
            do {
                const auto& a = nodes[occurrences[o].node].point;
                const auto& b = nodes[occurrences[occurrences[o].next].node].point;
                if constexpr (std::is_same_v<Integer, NativeInteger>) {
                    // Translate this closed sum exactly, retaining original
                    // coordinates for every predicate and output byte.
                    const auto& origin = nodes[first_node].point;
                    rings.back().area +=
                            (a.ex - origin.ex) * (b.ey - origin.ey) - (a.ey - origin.ey) * (b.ex - origin.ex);
                } else {
                    rings.back().area += a.ex * b.ey - a.ey * b.ex;
                }
                o = occurrences[o].next;
            } while (o != first);
            if (rings.back().area == 0) invalid("ST_CoverageSimplify zero-area polygon ring");
        }
    }
    void decode() {
        node_map.reserve(input_bytes / 16);
        for (uint32_t r = 0; r < rows.size(); ++r) {
            auto& row = rows[r];
            row.first = components.size();
            if (row.null) continue;
            Reader reader(Slice(reinterpret_cast<const char*>(row.bytes.data()), row.bytes.size()));
            auto [little, type] = reader.header();
            row.multi = type == 6;
            if (!row.multi)
                polygon(reader, little);
            else {
                uint32_t n = reader.number<uint32_t>(little);
                for (uint32_t i = 0; i < n; ++i) {
                    auto [order, child] = reader.header();
                    polygon(reader, order);
                }
            }
            row.count = components.size() - row.first;
            reader.done();
        }
        // Occurrences are complete after seam deduplication. Preserve their
        // encounter order in one flat adjacency array, without per-node heaps.
        node_occurrence_ids.resize(occurrences.size());
        for (const auto& occurrence : occurrences) {
            ++nodes[occurrence.node].count;
        }
        uint32_t first = 0;
        for (auto& node : nodes) {
            node.first = first;
            first += node.count;
            node.count = 0;
        }
        for (uint32_t o = 0; o < occurrences.size(); ++o) {
            auto& node = nodes[occurrences[o].node];
            node_occurrence_ids[node.first + node.count++] = o;
        }
    }
    void add_edge(uint32_t occurrence, bool indexed = true) {
        uint32_t a = occurrences[occurrence].node, b = occurrences[occurrences[occurrence].next].node;
        auto [it, fresh] = edge_map.emplace(key(a, b), 0);
        if (fresh) {
            it->second = index(edges.size());
            edges.push_back({std::min(a, b), std::max(a, b)});
            if (indexed) edge_tree.insert({bounds(nodes[a].point, nodes[b].point), it->second});
        }
        auto& edge = edges[it->second];
        if (edge.count == 2) invalid("ST_CoverageSimplify more than two coverage edge owners");
        edge.owners[edge.count++] = occurrence;
    }
    void erase_edge(uint32_t id) {
        auto& edge = edges[id];
        if (edge_tree.is_grid())
            edge_tree.erase(id);
        else
            edge_tree.erase_tree({bounds(nodes[edge.a].point, nodes[edge.b].point), id});
        edge_map.erase(key(edge.a, edge.b));
        edge.active = false;
    }
    void graph() {
        edge_map.reserve(occurrences.size());
        for (uint32_t o = 0; o < occurrences.size(); ++o) add_edge(o, false);
        std::vector<Item> items;
        items.reserve(edges.size());
        for (uint32_t i = 0; i < edges.size(); ++i) {
            items.push_back({bounds(nodes[edges[i].a].point, nodes[edges[i].b].point), i});
        }
        std::vector<Item> points;
        points.reserve(nodes.size());
        for (uint32_t n = 0; n < nodes.size(); ++n) points.push_back({bounds(nodes[n].point, nodes[n].point), n});
        auto edges_grid = Grid::layout(items, true), points_grid = Grid::layout(points, false);
        const bool use_grid =
                edges_grid.fits(items, items, nodes.size()) && points_grid.fits(points, items, nodes.size());
        edge_tree.build(items, use_grid ? std::move(edges_grid) : Grid{}, nodes.size());
        node_tree.build(points, use_grid ? std::move(points_grid) : Grid{});
        std::vector<unsigned> degree(nodes.size(), 0);
        for (const auto& edge : edges) {
            ++degree[edge.a];
            ++degree[edge.b];
        }
        for (size_t i = 0; i < nodes.size(); ++i) {
            nodes[i].fixed = degree[i] != 2;
        }
    }
    std::span<const uint32_t> node_occurrences(uint32_t n) const {
        return std::span<const uint32_t>(node_occurrence_ids).subspan(nodes[n].first, nodes[n].count);
    }
    bool candidate(uint32_t n, bool boundary, uint32_t& a, uint32_t& c) {
        auto& node = nodes[n];
        if (node.fixed || !node.active || node.count == 0) return false;
        bool first = true;
        for (uint32_t o : node_occurrences(n)) {
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
        Integer area = abs(orientation(nodes[a].point, nodes[n].point, nodes[c].point));
        if (area < floor) area = floor;
        heap.push_back({std::move(area), n, nodes[n].version});
        std::push_heap(heap.begin(), heap.end(), CandidateLess{});
    }
    bool removable(uint32_t n, uint32_t a, uint32_t c) {
        const auto& p = nodes[a].point;
        const auto& q = nodes[n].point;
        const auto& r = nodes[c].point;
        uint32_t left = edge_map.at(key(a, n)), right = edge_map.at(key(n, c));
        bool safe = true;
        query(edge_tree, bounds(p, r), [&](uint32_t id) {
            if (id == left || id == right || !edges[id].active || !safe) return;
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
            node_tree.visit(
                    box,
                    [&](uint32_t id) {
                        if (id == a || id == n || id == c || !nodes[id].active || !safe) return;
                        const auto& point = nodes[id].point;
                        int x = sign(orientation(p, q, point)), y = sign(orientation(q, r, point)),
                            z = sign(orientation(r, p, point));
                        if ((x >= 0 && y >= 0 && z >= 0) || (x <= 0 && y <= 0 && z <= 0)) safe = false;
                    },
                    [&](uint32_t id) { return nodes[id].active; });
            if (!safe) return false;
        }
        for (uint32_t o : node_occurrences(n)) {
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
        for (uint32_t o : node_occurrences(n)) {
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
        node_tree.erase(n, nodes[n].point.x, nodes[n].point.y);
        nodes[n].active = false;
        ++nodes[a].version;
        ++nodes[c].version;
    }
    void simplify(double tolerance, bool boundary) {
        ++calls;
        const Integer t = scaled(tolerance), threshold = 2 * t * t;
        Integer floor = 0;
        for (uint32_t n = 0; n < nodes.size(); ++n) {
            enqueue(n, boundary, floor);
        }
        while (!heap.empty()) {
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
    }
    void emit_polygon(std::vector<uint8_t>& output, const Component& component) {
        write<uint8_t>(output, 1);
        write<uint32_t>(output, 3);
        write<uint32_t>(output, component.count);
        for (uint32_t r = 0; r < component.count; ++r) {
            const auto& ring = rings[component.first + r];
            write<uint32_t>(output, ring.count + 1);
            uint32_t o = ring.first;
            do {
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
    }
};

} // namespace

struct GeoCoverageSimplify::Impl : Partition {
    // Dispatch once,
    // only after every row and tolerance have contributed to the same bound.
    std::variant<std::monostate, Kernel<NativeInteger>, Kernel<FixedInteger>, Kernel<DynamicInteger>> kernel;
    void run(double tolerance, bool boundary) {
        arithmetic.include(tolerance);
        if (arithmetic.native(tolerance))
            kernel.emplace<1>(*this, arithmetic.common_shift).run(tolerance, boundary);
        else if (arithmetic.fixed())
            kernel.emplace<2>(*this, arithmetic.common_shift).run(tolerance, boundary);
        else
            kernel.emplace<3>(*this, 0).run(tolerance, boundary);
    }
};

GeoCoverageSimplify::GeoCoverageSimplify(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
GeoCoverageSimplify::~GeoCoverageSimplify() = default;

StatusOr<std::unique_ptr<GeoCoverageSimplify>> GeoCoverageSimplify::create() {
    try {
        auto impl = std::make_unique<Impl>();
        return std::unique_ptr<GeoCoverageSimplify>(new GeoCoverageSimplify(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("ST_CoverageSimplify state allocation failed");
    }
}
Status GeoCoverageSimplify::append(std::optional<Slice> wkb) {
    auto& s = *_impl;
    if (s.finished || s.failed) return Status::InvalidArgument("ST_CoverageSimplify partition is closed");
    try {
        index(s.rows.size());
        if (wkb) {
            Reader reader(*wkb);
            s.preflight(reader, false);
            reader.done();
        }
        s.rows.emplace_back();
        auto& row = s.rows.back();
        row.null = !wkb;
        if (wkb) {
            row.bytes.assign(reinterpret_cast<const uint8_t*>(wkb->data),
                             reinterpret_cast<const uint8_t*>(wkb->data) + wkb->size);
            s.input_bytes += wkb->size;
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
        s.null_parameter = !tolerance || !boundary;
        if (!s.null_parameter) {
            if (!std::isfinite(*tolerance) || *tolerance < 0 || !std::isfinite(*tolerance * *tolerance))
                invalid("ST_CoverageSimplify requires finite nonnegative tolerance and representable tolerance "
                        "squared");
            s.run(*tolerance, *boundary);
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
size_t GeoCoverageSimplify::rows() const {
    return _impl->rows.size();
}
size_t GeoCoverageSimplify::kernel_calls() const {
    return _impl->calls;
}
} // namespace starrocks
