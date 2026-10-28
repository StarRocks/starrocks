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

#include "geo/geo_topology_simplify.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <map>
#include <numeric>
#include <set>
#include <vector>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/geometry.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include "geo/wkb.h"

namespace starrocks {
namespace {
namespace bg = boost::geometry;
namespace bgi = bg::index;
constexpr auto kCartesian = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;

// Each finite binary64 coordinate, scaled by 2^1074, is an integer <2^2098.
// Differences are <2^2099, dot/cross products <2^4199, and the squared
// distance comparisons below <2^8398. Ring sums add at most 13 bits. 16384
// checked bits therefore cover every expression, including doubled samples.
// Dynamic limbs use standard allocations (and the caller's BE memory hooks).
using Integer = boost::multiprecision::number<
        boost::multiprecision::cpp_int_backend<128, 16384, boost::multiprecision::signed_magnitude,
                                               boost::multiprecision::checked,
                                               std::allocator<boost::multiprecision::limb_type>>,
        boost::multiprecision::et_off>;
struct ExactPoint {
    Integer x;
    Integer y;
};

Integer scaled(double value) {
    const uint64_t bits = std::bit_cast<uint64_t>(value);
    const uint64_t exponent = (bits >> 52) & 0x7ff;
    Integer result = bits & ((uint64_t{1} << 52) - 1);
    if (exponent != 0) {
        result += uint64_t{1} << 52;
        result <<= exponent - 1;
    }
    return bits >> 63 ? -result : result;
}
ExactPoint exact(WkbCoordinate p) {
    return {scaled(p.x), scaled(p.y)};
}
Integer orientation(const ExactPoint& a, const ExactPoint& b, const ExactPoint& c) {
    return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
}
int sign(const Integer& value) {
    return (value > 0) - (value < 0);
}
bool less(WkbCoordinate a, WkbCoordinate b) {
    return a.x < b.x || (a.x == b.x && a.y < b.y);
}

struct Stop {
    Status status;
};
class Budget {
public:
    Budget(size_t limit, const std::function<Status()>& checkpoint, size_t used = 0)
            : _limit(limit), _checkpoint(checkpoint), _used(used) {
        check();
    }
    void charge(size_t count = 1) {
        if (_used > _limit || count > _limit - _used) {
            throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology exceeds work limit")};
        }
        _used += count;
        if (_used - _last_check >= 128) check();
    }
    void check() {
        _last_check = _used;
        if (_checkpoint) {
            Status result = _checkpoint();
            if (!result.ok()) throw Stop{std::move(result)};
        }
    }
    size_t used() const { return _used; }

private:
    size_t _limit;
    const std::function<Status()>& _checkpoint;
    size_t _used;
    size_t _last_check = 0;
};

enum class PathKind { POINT, LINE, RING };
struct Path {
    const std::vector<WkbCoordinate>* raw = nullptr;
    std::vector<ExactPoint> points;
    std::vector<bool> pinned;
    PathKind kind;
    int polygon = -1;
    int group = -1; // The particular MULTIPOLYGON ancestor, not collection-wide.
    int ring = -1;
    bool closed = false;
    Integer area2;
};
using Retained = std::vector<std::vector<uint32_t>>;
Retained all_vertices(const std::vector<Path>& paths) {
    Retained result(paths.size());
    for (size_t i = 0; i < paths.size(); ++i) {
        result[i].resize(paths[i].points.size());
        std::iota(result[i].begin(), result[i].end(), 0);
    }
    return result;
}
Integer ring_area(const Path& path, const std::vector<uint32_t>& kept, Budget& budget) {
    Integer area = 0;
    const auto& origin = path.points[kept.front()];
    for (size_t i = 1; i < kept.size(); ++i) {
        budget.charge();
        area += orientation(origin, path.points[kept[i - 1]], path.points[kept[i]]);
    }
    return area;
}
struct Edge {
    size_t path;
    uint32_t a;
    uint32_t b;
    size_t order;
};
struct Segment {
    const ExactPoint& a;
    const ExactPoint& b;
    WkbCoordinate raw_a;
    WkbCoordinate raw_b;
};
Segment segment(const Edge& edge, const std::vector<Path>& paths) {
    const auto& p = paths[edge.path];
    return {p.points[edge.a], p.points[edge.b], (*p.raw)[edge.a], (*p.raw)[edge.b]};
}
bool between(WkbCoordinate p, WkbCoordinate a, WkbCoordinate b) {
    return std::min(a.x, b.x) <= p.x && p.x <= std::max(a.x, b.x) && std::min(a.y, b.y) <= p.y &&
           p.y <= std::max(a.y, b.y);
}
enum class ContactKind { NONE, POINT, CROSS, OVERLAP };
struct Contact {
    ContactKind kind = ContactKind::NONE;
    WkbCoordinate point;
};
Contact contact(const Segment& first, const Segment& second, Budget& budget) {
    budget.charge();
    const int a = sign(orientation(first.a, first.b, second.a));
    const int b = sign(orientation(first.a, first.b, second.b));
    const int c = sign(orientation(second.a, second.b, first.a));
    const int d = sign(orientation(second.a, second.b, first.b));
    if (a == 0 && b == 0 && c == 0 && d == 0) {
        auto lo1 = less(first.raw_a, first.raw_b) ? first.raw_a : first.raw_b;
        auto hi1 = less(first.raw_a, first.raw_b) ? first.raw_b : first.raw_a;
        auto lo2 = less(second.raw_a, second.raw_b) ? second.raw_a : second.raw_b;
        auto hi2 = less(second.raw_a, second.raw_b) ? second.raw_b : second.raw_a;
        auto lo = less(lo1, lo2) ? lo2 : lo1;
        auto hi = less(hi1, hi2) ? hi1 : hi2;
        if (less(hi, lo)) return {};
        return {lo == hi ? ContactKind::POINT : ContactKind::OVERLAP, lo};
    }
    if (a * b < 0 && c * d < 0) return {ContactKind::CROSS, {}};
    if (a == 0 && between(second.raw_a, first.raw_a, first.raw_b)) return {ContactKind::POINT, second.raw_a};
    if (b == 0 && between(second.raw_b, first.raw_a, first.raw_b)) return {ContactKind::POINT, second.raw_b};
    if (c == 0 && between(first.raw_a, second.raw_a, second.raw_b)) return {ContactKind::POINT, first.raw_a};
    if (d == 0 && between(first.raw_b, second.raw_a, second.raw_b)) return {ContactKind::POINT, first.raw_b};
    return {};
}

// Every input double is represented exactly in long double. Wider exponent
// range also keeps the R-tree's box-area/split arithmetic finite at DBL_MAX;
// topology decisions still use the exact predicates, never these box areas.
using BoxPoint = bg::model::d2::point_xy<long double>;
using Box = bg::model::box<BoxPoint>;
using Item = std::pair<Box, size_t>;
using Tree = bgi::rtree<Item, bgi::quadratic<16>>;
Box bounds(WkbCoordinate a, WkbCoordinate b) {
    return {{std::min(a.x, b.x), std::min(a.y, b.y)}, {std::max(a.x, b.x), std::max(a.y, b.y)}};
}
struct Index {
    std::vector<Edge> edges;
    std::vector<size_t> counts;
    Tree tree;
    void rebuild(const std::vector<Path>& paths, const Retained& kept, Budget& budget) {
        budget.check();
        edges.clear();
        tree.clear();
        counts.assign(paths.size(), 0);
        for (size_t i = 0; i < paths.size(); ++i) {
            for (size_t j = 1; j < kept[i].size(); ++j) {
                const auto a = kept[i][j - 1], b = kept[i][j];
                if ((*paths[i].raw)[a] == (*paths[i].raw)[b]) continue;
                edges.push_back({i, a, b, counts[i]++});
            }
            if (paths[i].kind == PathKind::POINT && !kept[i].empty()) {
                edges.push_back({i, kept[i][0], kept[i][0], counts[i]++});
            }
        }
        budget.charge(edges.size());
        for (size_t i = 0; i < edges.size(); ++i) {
            auto s = segment(edges[i], paths);
            tree.insert({bounds(s.raw_a, s.raw_b), i});
        }
        budget.check();
    }
    std::vector<Item> query(const Box& box, Budget& budget) const {
        // Charge a conservative leaf-scan bound, including an empty result.
        budget.charge(edges.size());
        std::vector<Item> result;
        tree.query(bgi::intersects(box), std::back_inserter(result));
        return result;
    }
    bool adjacent(const Edge& a, const Edge& b, const std::vector<Path>& paths) const {
        if (a.path != b.path) return false;
        const auto lo = std::min(a.order, b.order), hi = std::max(a.order, b.order);
        return hi == lo + 1 || (paths[a.path].closed && lo == 0 && hi + 1 == counts[a.path]);
    }
};

// A midpoint is represented doubled, so even subnormal input edges get an
// exact test point; no rounded midpoint or arbitrary epsilon enters topology.
// -1 outside, 0 boundary, 1 inside.
int location(const ExactPoint& sample, int factor, const Path& ring, const std::vector<uint32_t>& kept,
             Budget& budget) {
    bool inside = false;
    for (size_t i = 1; i < kept.size(); ++i) {
        budget.charge();
        const auto& a = ring.points[kept[i - 1]];
        const auto& b = ring.points[kept[i]];
        const Integer x = sample.x - a.x * factor, y = sample.y - a.y * factor;
        const Integer side = (b.x - a.x) * y - (b.y - a.y) * x;
        if (side == 0 && std::min(a.x, b.x) * factor <= sample.x && sample.x <= std::max(a.x, b.x) * factor &&
            std::min(a.y, b.y) * factor <= sample.y && sample.y <= std::max(a.y, b.y) * factor) {
            return 0;
        }
        if ((a.y * factor > sample.y) != (b.y * factor > sample.y) && sign(side) == sign(b.y - a.y)) {
            inside = !inside;
        }
    }
    return inside ? 1 : -1;
}
int ring_relation(const Path& a, const std::vector<uint32_t>& ka, const Path& b, const std::vector<uint32_t>& kb,
                  Budget& budget) {
    int result = 0;
    for (size_t i = 1; i < ka.size(); ++i) {
        auto& p = a.points[ka[i - 1]];
        auto& q = a.points[ka[i]];
        if ((*a.raw)[ka[i - 1]] == (*a.raw)[ka[i]]) continue;
        const int next = location({p.x + q.x, p.y + q.y}, 2, b, kb, budget);
        if (next == 0) continue;
        if (result != 0 && result != next) {
            throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology polygon rings cross")};
        }
        result = next;
    }
    return result;
}

// Ring/contact-point incidence graph, rather than ring/ring graph: several
// rings may validly touch at one point without creating a false graph cycle.
class RingContacts {
public:
    explicit RingContacts(size_t rings) : _parent(rings) { std::iota(_parent.begin(), _parent.end(), 0); }
    void add(int polygon, size_t ring, WkbCoordinate point) {
        auto [it, fresh] = _points.emplace(Key{polygon, point}, _parent.size());
        if (fresh) _parent.push_back(_parent.size());
        const size_t node = it->second;
        if (!_links.emplace(ring, node).second) return;
        const size_t a = root(ring), b = root(node);
        if (a == b) throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology polygon interior is disconnected")};
        _parent[a] = b;
    }

private:
    struct Key {
        int polygon;
        WkbCoordinate point;
        bool operator<(const Key& other) const {
            return polygon < other.polygon || (polygon == other.polygon && less(point, other.point));
        }
    };
    size_t root(size_t n) {
        while (_parent[n] != n) {
            _parent[n] = _parent[_parent[n]];
            n = _parent[n];
        }
        return n;
    }
    std::vector<size_t> _parent;
    std::map<Key, size_t> _points;
    std::set<std::pair<size_t, size_t>> _links;
};

bool polygon_pair(const Path& a, const Path& b) {
    return a.kind == PathKind::RING && b.kind == PathKind::RING &&
           (a.polygon == b.polygon || (a.group >= 0 && a.group == b.group));
}
void validate_paths(const std::vector<Path>& paths, const Retained& kept, const Index& index, Budget& budget,
                    std::vector<Path>* pin_target = nullptr) {
    RingContacts graph(paths.size());
    for (size_t i = 0; i < paths.size(); ++i) {
        const auto& p = paths[i];
        if (p.kind == PathKind::RING) {
            if (kept[i].size() < 4 || ring_area(p, kept[i], budget) == 0) {
                throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology polygon ring is degenerate")};
            }
        } else if (p.kind == PathKind::LINE && index.counts[i] == 0) {
            throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology LINESTRING is degenerate")};
        }
    }
    for (size_t i = 0; i < index.edges.size(); ++i) {
        const auto& a = index.edges[i];
        auto sa = segment(a, paths);
        for (const auto& item : index.query(bounds(sa.raw_a, sa.raw_b), budget)) {
            if (item.second <= i) continue;
            const auto& b = index.edges[item.second];
            const auto hit = contact(sa, segment(b, paths), budget);
            if (hit.kind == ContactKind::NONE) continue;
            if (hit.kind == ContactKind::POINT && index.adjacent(a, b, paths)) continue;
            const auto& pa = paths[a.path];
            const auto& pb = paths[b.path];
            if (pa.kind == PathKind::RING && a.path == b.path) {
                throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology polygon ring is not simple")};
            }
            if (polygon_pair(pa, pb)) {
                if (hit.kind != ContactKind::POINT) {
                    throw Stop{
                            Status::InvalidArgument("ST_SimplifyPreserveTopology polygon boundaries cross or overlap")};
                }
                if (pa.polygon == pb.polygon) {
                    graph.add(pa.polygon, a.path, hit.point);
                    graph.add(pa.polygon, b.path, hit.point);
                }
            }
            if (pin_target != nullptr) {
                (*pin_target)[a.path].pinned[a.a] = (*pin_target)[a.path].pinned[a.b] = true;
                (*pin_target)[b.path].pinned[b.a] = (*pin_target)[b.path].pinned[b.b] = true;
            }
        }
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        const auto& a = paths[i];
        if (a.kind != PathKind::RING) continue;
        for (size_t j = i + 1; j < paths.size(); ++j) {
            const auto& b = paths[j];
            if (!polygon_pair(a, b)) continue;
            const int ab = ring_relation(a, kept[i], b, kept[j], budget);
            const int ba = ring_relation(b, kept[j], a, kept[i], budget);
            if (a.polygon == b.polygon) {
                const bool valid = a.ring == 0 ? (ab == -1 && ba == 1)
                                               : b.ring == 0 ? (ab == 1 && ba == -1) : (ab == -1 && ba == -1);
                if (!valid)
                    throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology hole ownership is invalid")};
            }
        }
    }
    // A MULTIPOLYGON member may be inside another member's hole, but not its
    // filled interior. Check every exterior edge midpoint against that interior.
    for (size_t i = 0; i < paths.size(); ++i) {
        const auto& a = paths[i];
        if (a.kind != PathKind::RING || a.ring != 0 || a.group < 0) continue;
        for (size_t j = 0; j < paths.size(); ++j) {
            const auto& b = paths[j];
            if (b.kind != PathKind::RING || b.ring != 0 || a.polygon == b.polygon || a.group != b.group) continue;
            for (size_t k = 1; k < kept[i].size(); ++k) {
                auto& p = a.points[kept[i][k - 1]];
                auto& q = a.points[kept[i][k]];
                if ((*a.raw)[kept[i][k - 1]] == (*a.raw)[kept[i][k]]) continue;
                ExactPoint sample{p.x + q.x, p.y + q.y};
                if (location(sample, 2, b, kept[j], budget) != 1) continue;
                bool in_hole = false;
                for (size_t h = 0; h < paths.size(); ++h) {
                    if (paths[h].polygon == b.polygon && paths[h].ring > 0 &&
                        location(sample, 2, paths[h], kept[h], budget) >= 0) {
                        in_hole = true;
                        break;
                    }
                }
                if (!in_hole)
                    throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology MULTIPOLYGON interiors overlap")};
            }
        }
    }
    budget.check();
}

bool within_distance(const ExactPoint& p, const ExactPoint& a, const ExactPoint& c, const Integer& tolerance2,
                     unsigned distance_shift) {
    // Every final chord is certified against ALL vertices of its original
    // subchain, including earlier deletions. Its closed tolerance capsule is
    // convex, so the original segments also lie in it. Conversely, continuous
    // projection of that connected subchain covers the entire chord (same
    // endpoints), giving the reverse Hausdorff bound. No error is accumulated.
    const Integer ux = c.x - a.x, uy = c.y - a.y, vx = p.x - a.x, vy = p.y - a.y;
    const Integer length2 = ux * ux + uy * uy, projection = vx * ux + vy * uy;
    if (projection <= 0) return ((vx * vx + vy * vy) << distance_shift) <= tolerance2;
    if (projection >= length2) {
        const Integer x = p.x - c.x, y = p.y - c.y;
        return ((x * x + y * y) << distance_shift) <= tolerance2;
    }
    const Integer cross = ux * vy - uy * vx;
    return ((cross * cross) << distance_shift) <= tolerance2 * length2;
}

Integer divide_power_of_two(const Integer& value, unsigned shift) {
    // Checked signed integers forbid bitwise shifts of a negative magnitude.
    return value < 0 ? -((-value) >> shift) : value >> shift;
}
unsigned common_scale(std::vector<Path>& paths, Budget& budget) {
    unsigned shift = 2098;
    for (const auto& path : paths) {
        for (const auto& p : path.points) {
            budget.charge();
            for (const auto* value : {&p.x, &p.y}) {
                if (*value != 0)
                    shift = std::min<unsigned>(shift, boost::multiprecision::lsb(*value < 0 ? -*value : *value));
            }
        }
    }
    if (shift == 2098) shift = 0;
    for (auto& path : paths) {
        for (auto& p : path.points) {
            p.x = divide_power_of_two(p.x, shift);
            p.y = divide_power_of_two(p.y, shift);
        }
    }
    // This is exact division by a shared factor, without origin translation.
    return shift;
}
bool in_triangle(const ExactPoint& p, const ExactPoint& a, const ExactPoint& b, const ExactPoint& c) {
    const int first = sign(orientation(a, b, p)), second = sign(orientation(b, c, p)),
              third = sign(orientation(c, a, p));
    return (first >= 0 && second >= 0 && third >= 0) || (first <= 0 && second <= 0 && third <= 0);
}

void append_paths(const WkbGeometry& geometry, int group, int& next_polygon, int& next_group, size_t& components,
                  size_t& coordinates, std::vector<Path>& paths, Budget& budget) {
    budget.charge();
    if (++components > kGeoTopologySimplifyMaxComponents) {
        throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology exceeds component limit")};
    }
    if (geometry.empty) return;
    auto add = [&](const std::vector<WkbCoordinate>& raw, PathKind kind, int polygon, int ring) {
        if (raw.size() > kGeoTopologySimplifyMaxCoordinates - coordinates) {
            throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology exceeds coordinate limit")};
        }
        coordinates += raw.size();
        budget.check();
        Path path;
        path.raw = &raw;
        path.kind = kind;
        path.polygon = polygon;
        path.group = group;
        path.ring = ring;
        path.closed = raw.size() > 1 && raw.front() == raw.back();
        path.points.reserve(raw.size());
        path.pinned.assign(raw.size(), false);
        for (const auto p : raw) {
            budget.charge();
            path.points.push_back(exact(p));
        }
        paths.emplace_back(std::move(path));
    };
    if (geometry.type == WkbGeometryType::POINT || geometry.type == WkbGeometryType::LINESTRING) {
        add(geometry.coordinates, geometry.type == WkbGeometryType::POINT ? PathKind::POINT : PathKind::LINE, -1, -1);
    } else if (geometry.type == WkbGeometryType::POLYGON) {
        const int polygon = next_polygon++;
        for (size_t i = 0; i < geometry.rings.size(); ++i) {
            if (++components > kGeoTopologySimplifyMaxComponents) {
                throw Stop{Status::InvalidArgument("ST_SimplifyPreserveTopology exceeds component limit")};
            }
            add(geometry.rings[i], PathKind::RING, polygon, i);
        }
    } else {
        const int child_group = geometry.type == WkbGeometryType::MULTIPOLYGON ? next_group++ : -1;
        for (const auto& child : geometry.children) {
            append_paths(child, child_group, next_polygon, next_group, components, coordinates, paths, budget);
        }
    }
}

void reconstruct(WkbGeometry& output, const std::vector<Path>& paths, const Retained& retained, size_t& next) {
    if (output.empty) return;
    auto replace = [&](std::vector<WkbCoordinate>& coordinates) {
        const auto& path = paths[next];
        const auto& kept = retained[next++];
        std::vector<WkbCoordinate> result;
        result.reserve(kept.size());
        for (const auto i : kept) result.push_back((*path.raw)[i]);
        coordinates = std::move(result);
    };
    if (output.type == WkbGeometryType::POINT || output.type == WkbGeometryType::LINESTRING) {
        replace(output.coordinates);
    } else if (output.type == WkbGeometryType::POLYGON) {
        for (auto& ring : output.rings) replace(ring);
    } else {
        for (auto& child : output.children) reconstruct(child, paths, retained, next);
    }
}

bool removable(const std::vector<Path>& paths, const Index& index, Retained& retained, std::vector<Integer>& areas,
               size_t path_id, size_t position, const Integer& tolerance2, unsigned distance_shift, Budget& budget) {
    const auto& path = paths[path_id];
    auto& kept = retained[path_id];
    const auto a = kept[position - 1], b = kept[position], c = kept[position + 1];
    if (path.pinned[b] || (*path.raw)[a] == (*path.raw)[c]) return false;
    const auto& A = path.points[a];
    const auto& B = path.points[b];
    const auto& C = path.points[c];
    for (size_t i = a; i <= c; ++i) {
        budget.charge();
        if (!within_distance(path.points[i], A, C, tolerance2, distance_shift)) return false;
    }
    // The closed A-B-C triangle is the swept region of this local deformation.
    // Original non-adjacent contact edges have both endpoints pinned, hence
    // cannot change. A remaining foreign edge cannot enter this triangle through
    // A-B/B-C (that would be a preserved contact), through A-C (checked below),
    // or lie wholly inside it (its endpoints are checked below). The deformation
    // therefore changes neither crossings/overlaps nor part containment. This
    // includes other edges of this path, points, holes, islands, and collection
    // leaves. Endpoint-only incidence at A/C stays in place. A collinear removal
    // has no swept area; the chord/overlap and nonzero-chord checks suffice.
    // Ring sign and minimum size additionally exclude dimension collapse. These
    // properties compose over deletions; final exact validity is checked again.
    const Integer sweep_area = orientation(A, B, C);
    const Integer updated_area = areas[path_id] - sweep_area;
    if (path.kind == PathKind::RING && sign(updated_area) != sign(areas[path_id])) return false;
    Segment chord{A, C, (*path.raw)[a], (*path.raw)[c]};
    Box box = bounds(chord.raw_a, chord.raw_b);
    bg::expand(box, BoxPoint((*path.raw)[b].x, (*path.raw)[b].y));
    for (const auto& item : index.query(box, budget)) {
        const auto& e = index.edges[item.second];
        if (e.path == path_id && ((e.a == a && e.b == b) || (e.a == b && e.b == c))) continue;
        auto other = segment(e, paths);
        const auto hit = contact(chord, other, budget);
        if (hit.kind != ContactKind::NONE &&
            (hit.kind != ContactKind::POINT || !(hit.point == chord.raw_a || hit.point == chord.raw_b))) {
            return false;
        }
        if (sweep_area != 0) {
            for (const auto vertex : {e.a, e.b}) {
                const auto raw = (*paths[e.path].raw)[vertex];
                budget.charge();
                if (!(raw == chord.raw_a || raw == chord.raw_b) && in_triangle(paths[e.path].points[vertex], A, B, C)) {
                    return false;
                }
            }
        }
    }
    kept.erase(kept.begin() + position);
    areas[path_id] = updated_area;
    return true;
}
} // namespace

struct PreparedGeoTopologySimplify::Impl {
    // Allocations are bounded before construction: the WKB reader counts
    // declared children as well as nodes/rings/coordinates; paths have <=5000
    // positions and <=1024 components. There are <=5000 active edges/items,
    // query results, and retained indices. Exact coordinates use <=2098 bits
    // each; all integer temporaries have a checked 16384-bit maximum. The
    // ring/contact incidence graph is a forest (a cycle is rejected immediately),
    // hence has O(rings) nodes/links, not a quadratic allocation of all contacts.
    // No all-pairs contact matrix or tolerance-dependent result cache is kept.
    // Standard allocator bytes are charged by BE memory hooks; checkpoints run
    // before bounded allocation groups and through charged loops. Work units
    // bound these loops, not wall-clock time or Boost internals' instruction count.
    std::string wkb;
    WkbGeometry geometry;
    std::vector<Path> paths;
    size_t preparation_work = 0;
    size_t work_limit = kGeoTopologySimplifyMaxWork;
    unsigned coordinate_shift = 0;
};

PreparedGeoTopologySimplify::PreparedGeoTopologySimplify(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
PreparedGeoTopologySimplify::~PreparedGeoTopologySimplify() = default;

StatusOr<std::unique_ptr<PreparedGeoTopologySimplify>> PreparedGeoTopologySimplify::prepare(
        Slice wkb, const std::function<Status()>& checkpoint, size_t work_limit) {
    try {
        Budget budget(std::min(work_limit, kGeoTopologySimplifyMaxWork), checkpoint);
        budget.charge(wkb.size);
        if (wkb.size > kGeoTopologySimplifyMaxBytes) {
            return Status::InvalidArgument("ST_SimplifyPreserveTopology input exceeds 256 KiB");
        }
        auto impl = std::make_unique<Impl>();
        // Lower the shared reader's allocation checks before any declared reserve.
        RETURN_IF_ERROR(WkbCodec::parse_wkb_bounded(
                wkb, &impl->geometry, kGeoTopologySimplifyMaxCoordinates + kGeoTopologySimplifyMaxComponents,
                kCartesian));
        budget.check();
        impl->wkb.assign(wkb.data, wkb.size);
        impl->work_limit = std::min(work_limit, kGeoTopologySimplifyMaxWork);
        size_t components = 0, coordinates = 0;
        int polygon = 0, group = 0;
        append_paths(impl->geometry, -1, polygon, group, components, coordinates, impl->paths, budget);
        impl->coordinate_shift = common_scale(impl->paths, budget);
        const auto retained = all_vertices(impl->paths);
        Index index;
        index.rebuild(impl->paths, retained, budget);
        validate_paths(impl->paths, retained, index, budget, &impl->paths);
        for (size_t i = 0; i < impl->paths.size(); ++i) {
            if (impl->paths[i].kind == PathKind::RING) {
                impl->paths[i].area2 = ring_area(impl->paths[i], retained[i], budget);
            }
        }
        impl->preparation_work = budget.used();
        budget.check();
        return std::unique_ptr<PreparedGeoTopologySimplify>(new PreparedGeoTopologySimplify(std::move(impl)));
    } catch (const Stop& stop) {
        return stop.status;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("ST_SimplifyPreserveTopology preparation allocation failed");
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("ST_SimplifyPreserveTopology preparation failed: ") + error.what());
    }
}

StatusOr<std::string> PreparedGeoTopologySimplify::simplify(double tolerance,
                                                            const std::function<Status()>& checkpoint) const {
    try {
        Budget budget(_impl->work_limit, checkpoint, _impl->preparation_work);
        if (!std::isfinite(tolerance) || tolerance < 0) {
            return Status::InvalidArgument("ST_SimplifyPreserveTopology requires a finite nonnegative tolerance");
        }
        if (tolerance == 0) return _impl->wkb;
        const Integer original_t = scaled(tolerance);
        const unsigned shared_shift =
                std::min<unsigned>(_impl->coordinate_shift, boost::multiprecision::lsb(original_t));
        const Integer t = original_t >> shared_shift, tolerance2 = t * t;
        const unsigned distance_shift = 2 * (_impl->coordinate_shift - shared_shift);
        auto retained = all_vertices(_impl->paths);
        std::vector<Integer> areas;
        areas.reserve(_impl->paths.size());
        for (const auto& path : _impl->paths) areas.push_back(path.area2);
        Index index;
        index.rebuild(_impl->paths, retained, budget);
        bool changed;
        do {
            changed = false;
            for (size_t i = 0; i < retained.size(); ++i) {
                const auto& path = _impl->paths[i];
                if (path.kind == PathKind::POINT) continue;
                for (size_t j = 1; j + 1 < retained[i].size();) {
                    budget.charge();
                    if (path.closed && retained[i].size() <= 4) break;
                    if (removable(_impl->paths, index, retained, areas, i, j, tolerance2, distance_shift, budget)) {
                        changed = true;
                        index.rebuild(_impl->paths, retained, budget);
                    } else {
                        ++j;
                    }
                }
            }
        } while (changed);
        // The same exact validator checks emitted paths, without floating repair.
        const auto& paths = _impl->paths;
        validate_paths(paths, retained, index, budget);
        WkbGeometry output = _impl->geometry;
        size_t next = 0;
        reconstruct(output, paths, retained, next);
        std::string wkb;
        RETURN_IF_ERROR(WkbCodec::to_wkb(output, &wkb, kCartesian));
        budget.check();
        return wkb;
    } catch (const Stop& stop) {
        return stop.status;
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("ST_SimplifyPreserveTopology evaluation allocation failed");
    } catch (const std::exception& error) {
        return Status::InvalidArgument(std::string("ST_SimplifyPreserveTopology evaluation failed: ") + error.what());
    }
}

} // namespace starrocks
