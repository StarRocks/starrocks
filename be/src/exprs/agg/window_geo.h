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
#include <cmath>
#include <limits>
#include <random>
#include <vector>

#include "column/column_helper.h"
#include "exprs/agg/window.h"
#include "geo/geo_types.h"

namespace starrocks {

// ============================================================================
// Shared helpers
// ============================================================================

namespace geo_window_detail {

// Extract (longitude, latitude) centroid from a geometry column row.
// Returns NaN pair for null or invalid geometries.
static inline std::pair<double, double> centroid_of(const Column* col, int64_t row) {
    constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
    if (col->is_null(row)) return {kNaN, kNaN};
    Datum d = col->get(row);
    Slice s = d.get_slice();
    std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(s.data, s.size));
    if (!shape) return {kNaN, kNaN};
    switch (shape->type()) {
    case GEO_SHAPE_POINT:
        return {static_cast<const GeoPoint*>(shape.get())->x(), static_cast<const GeoPoint*>(shape.get())->y()};
    default: {
        double x0, y0, x1, y1;
        if (geo_bounding_box(shape.get(), &x0, &y0, &x1, &y1)) return {(x0 + x1) * 0.5, (y0 + y1) * 0.5};
        return {kNaN, kNaN};
    }
    }
}

// Great-circle distance in metres.
static inline double dist_m(double x1, double y1, double x2, double y2) {
    double result = 0.0;
    GeoPoint::st_distance_sphere(x1, y1, x2, y2, &result);
    return result;
}

// ============================================================================
// DBSCAN
// ============================================================================

static inline void dbscan(const std::vector<double>& xs, const std::vector<double>& ys, const std::vector<bool>& valid,
                          double eps_m, int min_pts, std::vector<int32_t>& out) {
    int n = (int)xs.size();
    out.assign(n, -1); // -1 = noise

    // Pre-compute neighbour lists (O(n²) — acceptable for window partitions)
    std::vector<std::vector<int>> nbrs(n);
    for (int i = 0; i < n; ++i) {
        if (!valid[i]) continue;
        for (int j = i + 1; j < n; ++j) {
            if (!valid[j]) continue;
            if (dist_m(xs[i], ys[i], xs[j], ys[j]) <= eps_m) {
                nbrs[i].push_back(j);
                nbrs[j].push_back(i);
            }
        }
    }

    int cid = 0;
    std::vector<bool> visited(n, false);

    for (int i = 0; i < n; ++i) {
        if (visited[i] || !valid[i]) continue;
        if ((int)nbrs[i].size() < min_pts) continue; // potential noise

        // Expand cluster
        out[i] = cid;
        visited[i] = true;

        std::vector<int> queue(nbrs[i].begin(), nbrs[i].end());
        for (int qi = 0; qi < (int)queue.size(); ++qi) {
            int pt = queue[qi];
            if (!visited[pt]) {
                visited[pt] = true;
                out[pt] = cid;
                if ((int)nbrs[pt].size() >= min_pts) {
                    for (int nb : nbrs[pt]) {
                        if (!visited[nb]) queue.push_back(nb);
                    }
                }
            } else if (out[pt] < 0) {
                out[pt] = cid; // border point absorbed into cluster
            }
        }
        ++cid;
    }
}

// ============================================================================
// K-Means (k-means++ init + Lloyd's iterations, max 100 iters)
// ============================================================================

static inline void kmeans(const std::vector<double>& xs, const std::vector<double>& ys, const std::vector<bool>& valid,
                          int k, std::vector<int32_t>& out) {
    int n = (int)xs.size();
    out.assign(n, 0);
    if (k <= 0 || n == 0) return;
    k = std::min(k, n);

    // Collect valid indices
    std::vector<int> valid_idx;
    valid_idx.reserve(n);
    for (int i = 0; i < n; ++i)
        if (valid[i]) valid_idx.push_back(i);
    if (valid_idx.empty()) return;

    // k-means++ initialisation (deterministic seed for reproducibility)
    std::mt19937_64 rng(0xDEADBEEF42ULL);
    std::vector<double> cx(k), cy(k);

    {
        size_t first = rng() % valid_idx.size();
        cx[0] = xs[valid_idx[first]];
        cy[0] = ys[valid_idx[first]];
    }

    std::vector<double> min_d2(n, std::numeric_limits<double>::max());
    for (int ki = 1; ki < k; ++ki) {
        double total = 0.0;
        for (int vi : valid_idx) {
            double d = dist_m(xs[vi], ys[vi], cx[ki - 1], cy[ki - 1]);
            min_d2[vi] = std::min(min_d2[vi], d * d);
            total += min_d2[vi];
        }
        if (total <= 0.0) {
            cx[ki] = cx[0];
            cy[ki] = cy[0];
            continue;
        }
        double t = std::uniform_real_distribution<double>(0.0, total)(rng);
        double cum = 0.0;
        cx[ki] = cx[0];
        cy[ki] = cy[0];
        for (int vi : valid_idx) {
            cum += min_d2[vi];
            if (cum >= t) {
                cx[ki] = xs[vi];
                cy[ki] = ys[vi];
                break;
            }
        }
    }

    // Lloyd's iterations
    for (int iter = 0; iter < 100; ++iter) {
        bool changed = false;
        // Assignment
        for (int vi : valid_idx) {
            int best = 0;
            double best_d = std::numeric_limits<double>::max();
            for (int ki = 0; ki < k; ++ki) {
                double d = dist_m(xs[vi], ys[vi], cx[ki], cy[ki]);
                if (d < best_d) {
                    best_d = d;
                    best = ki;
                }
            }
            if (out[vi] != best) {
                out[vi] = best;
                changed = true;
            }
        }
        if (!changed) break;
        // Update centroids
        std::vector<double> sx(k, 0.0), sy(k, 0.0);
        std::vector<int> cnt(k, 0);
        for (int vi : valid_idx) {
            int ki = out[vi];
            sx[ki] += xs[vi];
            sy[ki] += ys[vi];
            ++cnt[ki];
        }
        for (int ki = 0; ki < k; ++ki) {
            if (cnt[ki] > 0) {
                cx[ki] = sx[ki] / cnt[ki];
                cy[ki] = sy[ki] / cnt[ki];
            }
        }
    }
}

} // namespace geo_window_detail

// ============================================================================
// ST_ClusterDBSCAN window function
// ============================================================================

struct ClusterDBSCANState {
    std::vector<int32_t> cluster_ids; // -1 = noise (→ NULL in output)
    int64_t cur_position{-1};         // 0-based within current partition
    bool computed{false};
};

// ST_ClusterDBSCAN(geom GEOMETRY, eps DOUBLE, minpoints INT) OVER (...) → INT (nullable)
//
// Usage:
//   SELECT id, ST_ClusterDBSCAN(geom, 1000.0, 3) OVER (PARTITION BY region) AS cluster_id
//   FROM t;
//
// eps is the neighbourhood radius in metres.
// minpoints is the minimum number of neighbours to form a core point.
// Returns NULL for noise points (fewer than minpoints neighbours within eps).
// Must be used with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (or no frame spec)
// to ensure the full partition is visible.
class ClusterDBSCANWindowFunction final : public WindowFunction<ClusterDBSCANState> {
public:
    std::string get_name() const override { return "st_clusterdbscan"; }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& s = this->data(state);
        s.cluster_ids.clear();
        s.cur_position = -1;
        s.computed = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        auto& s = this->data(state);

        if (!s.computed) {
            // Extract eps and minpoints from constant columns
            double eps_m = ColumnHelper::get_const_value<TYPE_DOUBLE>(columns[1]);
            int32_t min_pts = (int32_t)ColumnHelper::get_const_value<TYPE_INT>(columns[2]);

            int n = (int)(peer_group_end - peer_group_start);
            std::vector<double> xs(n), ys(n);
            std::vector<bool> valid(n, true);
            for (int i = 0; i < n; ++i) {
                auto [x, y] = geo_window_detail::centroid_of(columns[0], peer_group_start + i);
                xs[i] = x;
                ys[i] = y;
                valid[i] = !std::isnan(x) && !std::isnan(y);
            }
            geo_window_detail::dbscan(xs, ys, valid, eps_m, (int)min_pts, s.cluster_ids);
            s.computed = true;
        }
        ++s.cur_position;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_EQ(end, start + 1);
        const auto& s = this->data(state);

        int32_t cid = (s.cur_position < 0 || s.cur_position >= (int64_t)s.cluster_ids.size())
                              ? -1
                              : s.cluster_ids[s.cur_position];
        if (cid < 0) {
            dst->append_nulls(1);
        } else {
            dst->append_datum(Datum((int32_t)cid));
        }
    }
};

// ============================================================================
// ST_ClusterKMeans window function
// ============================================================================

struct ClusterKMeansState {
    std::vector<int32_t> cluster_ids; // 0-based cluster IDs
    int64_t cur_position{-1};
    bool computed{false};
};

// ST_ClusterKMeans(geom GEOMETRY, k INT) OVER (...) → INT
//
// Usage:
//   SELECT id, ST_ClusterKMeans(geom, 5) OVER (PARTITION BY region) AS cluster_id
//   FROM t;
//
// Partitions geometries into exactly k clusters using k-means (k-means++ init).
// Returns a 0-based cluster ID. Every row gets a non-NULL result (k-means assigns all points).
// Must be used with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
class ClusterKMeansWindowFunction final : public WindowFunction<ClusterKMeansState> {
public:
    std::string get_name() const override { return "st_clusterkmeans"; }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& s = this->data(state);
        s.cluster_ids.clear();
        s.cur_position = -1;
        s.computed = false;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        auto& s = this->data(state);

        if (!s.computed) {
            int32_t k = (int32_t)ColumnHelper::get_const_value<TYPE_INT>(columns[1]);

            int n = (int)(peer_group_end - peer_group_start);
            std::vector<double> xs(n), ys(n);
            std::vector<bool> valid(n, true);
            for (int i = 0; i < n; ++i) {
                auto [x, y] = geo_window_detail::centroid_of(columns[0], peer_group_start + i);
                xs[i] = x;
                ys[i] = y;
                valid[i] = !std::isnan(x) && !std::isnan(y);
            }
            geo_window_detail::kmeans(xs, ys, valid, (int)k, s.cluster_ids);
            s.computed = true;
        }
        ++s.cur_position;
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_EQ(end, start + 1);
        const auto& s = this->data(state);

        int32_t cid = (s.cur_position < 0 || s.cur_position >= (int64_t)s.cluster_ids.size())
                              ? 0
                              : s.cluster_ids[s.cur_position];
        // k-means always assigns a cluster (no noise), but be safe
        dst->append_datum(Datum((int32_t)cid));
    }
};

} // namespace starrocks
