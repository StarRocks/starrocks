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

package com.starrocks.metric;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Per-warehouse registry for slot-related observability metrics used by the
 * big-query scaling signal. Lazily creates and labels each metric on first
 * lookup; subsequent calls for the same warehouse id return the cached
 * instance so callers can safely update a stable handle.
 */
public class WarehouseSlotMetricMgr {
    private static final Logger LOG = LogManager.getLogger(WarehouseSlotMetricMgr.class);

    private static final Map<Long, GaugeMetricImpl<Long>> PENDING_MAX_RAW = new ConcurrentHashMap<>();
    private static final Map<Long, GaugeMetricImpl<Long>> PENDING_SUM_RAW = new ConcurrentHashMap<>();
    private static final Map<Long, GaugeMetricImpl<Long>> REQUIRED_CN = new ConcurrentHashMap<>();
    private static final Map<Long, GaugeMetricImpl<Double>> MAX_RAW_RATIO = new ConcurrentHashMap<>();
    private static final Map<Long, GaugeMetricImpl<Long>> PENDING_BIG_CNT = new ConcurrentHashMap<>();
    private static final Map<Long, LongCounterMetric> BIG_QUERY_TOTAL = new ConcurrentHashMap<>();
    private static final Map<Long, HistogramMetric> BIG_WAIT_HIST = new ConcurrentHashMap<>();
    private static final Map<Long, LongCounterMetric> PRE_SCALE_WAIT_TOTAL = new ConcurrentHashMap<>();
    private static final Map<Long, HistogramMetric> PRE_SCALE_WAIT_HIST = new ConcurrentHashMap<>();

    private WarehouseSlotMetricMgr() {
    }

    public static GaugeMetricImpl<Long> getMaxRawSlotsGauge(Long whId) {
        return getOrCreateMetric(whId, PENDING_MAX_RAW, () ->
                new GaugeMetricImpl<>("query_queue_pending_max_raw_slots",
                        Metric.MetricUnit.NOUNIT,
                        "max raw (un-clamped) estimated slots across pending queries per warehouse",
                        0L));
    }

    public static GaugeMetricImpl<Long> getSumRawSlotsGauge(Long whId) {
        return getOrCreateMetric(whId, PENDING_SUM_RAW, () ->
                new GaugeMetricImpl<>("query_queue_pending_sum_raw_slots",
                        Metric.MetricUnit.NOUNIT,
                        "sum of raw (un-clamped) estimated slots across pending queries per warehouse",
                        0L));
    }

    public static GaugeMetricImpl<Long> getRequiredComputeNodeGauge(Long whId) {
        return getOrCreateMetric(whId, REQUIRED_CN, () ->
                new GaugeMetricImpl<>("query_queue_required_compute_node_count",
                        Metric.MetricUnit.NOUNIT,
                        "ceil(max_raw_slots / slots_per_cn); intended for HPA desiredReplicas",
                        0L));
    }

    public static GaugeMetricImpl<Double> getMaxRawSlotsRatioGauge(Long whId) {
        return getOrCreateMetric(whId, MAX_RAW_RATIO, () ->
                new GaugeMetricImpl<>("query_queue_max_raw_slots_ratio",
                        Metric.MetricUnit.NOUNIT,
                        "max_pending_raw_slots / totalSlots; >1.0 means a pending query exceeds capacity",
                        0.0));
    }

    public static GaugeMetricImpl<Long> getPendingBigQueryCountGauge(Long whId) {
        return getOrCreateMetric(whId, PENDING_BIG_CNT, () ->
                new GaugeMetricImpl<>("query_queue_pending_big_query_count",
                        Metric.MetricUnit.REQUESTS,
                        "number of pending queries flagged as big per warehouse",
                        0L));
    }

    public static LongCounterMetric getBigQueryCounter(Long whId) {
        return getOrCreateMetric(whId, BIG_QUERY_TOTAL, () ->
                new LongCounterMetric("query_queue_big_query_total",
                        Metric.MetricUnit.REQUESTS,
                        "cumulative count of big queries observed at pending-enter per warehouse"));
    }

    public static HistogramMetric getBigQueryWaitHistogram(Long whId) {
        return getOrCreateHistogram(whId, BIG_WAIT_HIST, () ->
                new HistogramMetric("query_queue_pending_big_query_wait_ms"));
    }

    public static LongCounterMetric getPreScaleWaitCounter(Long whId) {
        return getOrCreateMetric(whId, PRE_SCALE_WAIT_TOTAL, () ->
                new LongCounterMetric("query_queue_pre_scale_wait_total",
                        Metric.MetricUnit.REQUESTS,
                        "cumulative count of pre-scale wait gate firings per warehouse"));
    }

    public static HistogramMetric getPreScaleWaitHistogram(Long whId) {
        return getOrCreateHistogram(whId, PRE_SCALE_WAIT_HIST, () ->
                new HistogramMetric("query_queue_pre_scale_wait_ms"));
    }

    private static <T extends Metric<?>> T getOrCreateMetric(Long whId, Map<Long, T> map, Supplier<T> creator) {
        T existing = map.get(whId);
        if (existing != null) {
            return existing;
        }
        synchronized (WarehouseSlotMetricMgr.class) {
            existing = map.get(whId);
            if (existing != null) {
                return existing;
            }
            T m = creator.get();
            attachWarehouseLabels(m, whId);
            map.put(whId, m);
            MetricRepo.addMetric(m);
            LOG.info("Registered warehouse slot metric {} for wh={}", m.getName(), whId);
            return m;
        }
    }

    private static HistogramMetric getOrCreateHistogram(Long whId, Map<Long, HistogramMetric> map,
                                                       Supplier<HistogramMetric> creator) {
        HistogramMetric existing = map.get(whId);
        if (existing != null) {
            return existing;
        }
        synchronized (WarehouseSlotMetricMgr.class) {
            existing = map.get(whId);
            if (existing != null) {
                return existing;
            }
            HistogramMetric h = creator.get();
            attachWarehouseLabels(h, whId);
            map.put(whId, h);
            // HistogramMetric extends com.codahale.metrics.Histogram, not Metric<?>, so it is
            // surfaced via MetricVisitor.visitHistogram from a dedicated collection path
            // (see B3) rather than the MetricRepo gauge/counter list.
            LOG.info("Registered warehouse slot histogram {} for wh={}", h.getName(), whId);
            return h;
        }
    }

    private static void attachWarehouseLabels(Metric<?> m, Long whId) {
        String whName = resolveWarehouseName(whId);
        m.addLabel(new MetricLabel("warehouse_id", whId.toString()));
        m.addLabel(new MetricLabel("warehouse_name", whName));
    }

    private static void attachWarehouseLabels(HistogramMetric h, Long whId) {
        String whName = resolveWarehouseName(whId);
        h.addLabel(new MetricLabel("warehouse_id", whId.toString()));
        h.addLabel(new MetricLabel("warehouse_name", whName));
    }

    private static String resolveWarehouseName(Long whId) {
        try {
            Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(whId);
            return wh == null ? "unknown" : wh.getName();
        } catch (Exception e) {
            // getWarehouse(long) throws ErrorReportException when the warehouse is absent
            // (typical in unit tests and right after FE startup before the registry is
            // populated). Fall back to "unknown" so metric registration is never blocked.
            return "unknown";
        }
    }
}
