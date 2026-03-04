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

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Manager for Iceberg metrics.
 * This class tracks metrics for Iceberg operations including delete, write, etc.
 */
public class IcebergMetricsMgr {

    // Iceberg delete metrics with delete_type label (position/metadata)
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_DELETE_TOTAL = new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_DELETE_DURATION_MS_TOTAL =
            new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_DELETE_BYTES = new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_DELETE_ROWS = new ConcurrentHashMap<>();

    // compaction metrics (rewrite_data_files)
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_COMPACTION_TOTAL =
            new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_COMPACTION_DURATION_MS =
            new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_COMPACTION_INPUT_FILES =
            new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_COMPACTION_OUTPUT_FILES =
            new ConcurrentHashMap<>();
    private static final Map<String, LongCounterMetric> COUNTER_ICEBERG_COMPACTION_REMOVED_DELETE_FILES =
            new ConcurrentHashMap<>();

    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILED = "failed";
    private static final String REASON_NONE = "none";
    private static final String REASON_UNKNOWN = "unknown";

    private static final String REASON_TIMEOUT = "timeout";
    private static final String REASON_OOM = "oom";
    private static final String REASON_ACCESS_DENIED = "access_denied";

    private static final String DELETE_TYPE_POSITION = "position";
    private static final String DELETE_TYPE_METADATA = "metadata";

    private static final String COMPACTION_TYPE_MANUAL = "manual";
    private static final String COMPACTION_TYPE_AUTO = "auto";

    /**
     * Record a completed Iceberg delete task.
     * This method automatically normalizes status, reason and deleteType.
     *
     * @param status     "success" or "failed" (will be normalized)
     * @param reason     failure reason: "none", "timeout", "oom", "access_denied", "unknown" (will be normalized)
     * @param deleteType "position" or "metadata" (will be normalized)
     */
    public static void increaseIcebergDeleteTotal(String status, String reason, String deleteType) {
        String normalizedStatus = normalizeStatus(status);
        String normalizedReason = normalizeReason(reason);
        String normalizedDeleteType = normalizeDeleteType(deleteType);

        String metricKey = normalizedStatus + "|" + normalizedReason + "|" + normalizedDeleteType;
        LongCounterMetric counter = COUNTER_ICEBERG_DELETE_TOTAL.computeIfAbsent(metricKey, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_delete_total", Metric.MetricUnit.REQUESTS,
                    "total iceberg delete tasks by status, reason and delete type");
            metric.addLabel(new MetricLabel("status", normalizedStatus));
            metric.addLabel(new MetricLabel("reason", normalizedReason));
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        });
        counter.increase(1L);
    }

    /**
     * Record a failed Iceberg delete task with automatic error classification.
     * This is a convenience method that combines error classification and metric recording.
     *
     * @param throwable  the throwable to classify for reason
     * @param deleteType "position" or "metadata"
     */
    public static void increaseIcebergDeleteTotalFail(Throwable throwable, String deleteType) {
        String reason = classifyFailReason(throwable);
        increaseIcebergDeleteTotal(STATUS_FAILED, reason, deleteType);
    }

    /**
     * Record a failed Iceberg delete task with automatic error classification from error message.
     * This is a convenience method that combines error classification and metric recording.
     *
     * @param errorMessage the error message to classify for reason
     * @param deleteType   "position" or "metadata"
     */
    public static void increaseIcebergDeleteTotalFail(String errorMessage, String deleteType) {
        String reason = classifyFailReason(errorMessage);
        increaseIcebergDeleteTotal(STATUS_FAILED, reason, deleteType);
    }

    /**
     * Record a successful Iceberg delete task.
     * This is a convenience method.
     *
     * @param deleteType "position" or "metadata"
     */
    public static void increaseIcebergDeleteTotalSuccess(String deleteType) {
        increaseIcebergDeleteTotal(STATUS_SUCCESS, REASON_NONE, deleteType);
    }

    /**
     * Record the duration of an Iceberg delete task.
     *
     * @param durationMs duration in milliseconds
     * @param deleteType "position" or "metadata"
     */
    public static void increaseIcebergDeleteDurationMsTotal(long durationMs, String deleteType) {
        String normalizedDeleteType = normalizeDeleteType(deleteType);
        LongCounterMetric counter = COUNTER_ICEBERG_DELETE_DURATION_MS_TOTAL.computeIfAbsent(normalizedDeleteType, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_delete_duration_ms_total",
                    Metric.MetricUnit.MILLISECONDS, "total duration in milliseconds of iceberg delete tasks by delete type");
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        });
        counter.increase(durationMs);
    }

    /**
     * Record the number of bytes deleted.
     * Only applicable to position delete.
     *
     * @param bytes      number of bytes deleted
     * @param deleteType "position" or "metadata"
     */
    public static void increaseIcebergDeleteBytes(long bytes, String deleteType) {
        String normalizedDeleteType = normalizeDeleteType(deleteType);
        LongCounterMetric counter = COUNTER_ICEBERG_DELETE_BYTES.computeIfAbsent(normalizedDeleteType, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_delete_bytes", Metric.MetricUnit.BYTES,
                    "total deleted bytes of iceberg delete tasks by delete type");
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        });
        counter.increase(bytes);
    }

    /**
     * Record the number of rows deleted.
     * Only applicable to position delete.
     *
     * @param rows       number of rows deleted
     * @param deleteType "position" or "metadata"
     */
    public static void increaseIcebergDeleteRows(long rows, String deleteType) {
        String normalizedDeleteType = normalizeDeleteType(deleteType);
        LongCounterMetric counter = COUNTER_ICEBERG_DELETE_ROWS.computeIfAbsent(normalizedDeleteType, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_delete_rows", Metric.MetricUnit.ROWS,
                    "total deleted rows of iceberg delete tasks by delete type");
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        });
        counter.increase(rows);
    }

    // Normalization and classification methods

    /**
     * Normalize status to "success", "failed", or "unknown".
     *
     * @param status the status to normalize
     * @return normalized status
     */
    public static String normalizeStatus(String status) {
        if (STATUS_SUCCESS.equalsIgnoreCase(status)) {
            return STATUS_SUCCESS;
        }
        if (STATUS_FAILED.equalsIgnoreCase(status)) {
            return STATUS_FAILED;
        }
        return status == null ? REASON_UNKNOWN : status;
    }

    /**
     * Normalize reason, returning "unknown" if null.
     *
     * @param reason the reason to normalize
     * @return normalized reason
     */
    public static String normalizeReason(String reason) {
        return reason == null ? REASON_UNKNOWN : reason;
    }

    /**
     * Normalize delete type to "position", "metadata", or "unknown".
     *
     * @param deleteType the delete type to normalize
     * @return normalized delete type
     */
    public static String normalizeDeleteType(String deleteType) {
        if (DELETE_TYPE_POSITION.equalsIgnoreCase(deleteType)) {
            return DELETE_TYPE_POSITION;
        }
        if (DELETE_TYPE_METADATA.equalsIgnoreCase(deleteType)) {
            return DELETE_TYPE_METADATA;
        }
        return deleteType == null ? REASON_UNKNOWN : deleteType;
    }

    /**
     * Classify the failure reason from a throwable.
     * This method combines error type checking and message analysis.
     *
     * @param throwable the throwable to classify
     * @return the classified reason: "oom", "access_denied", "timeout", or "unknown"
     */
    public static String classifyFailReason(Throwable throwable) {
        if (throwable == null) {
            return REASON_UNKNOWN;
        }
        // Check exception types first
        if (throwable instanceof OutOfMemoryError) {
            return "oom";
        }
        if (throwable instanceof AccessDeniedException) {
            return "access_denied";
        }
        if (throwable instanceof TimeoutException) {
            return "timeout";
        }
        // Fall back to message analysis
        return classifyFailReason(throwable.getMessage());
    }

    /**
     * Classify the failure reason from an error message.
     *
     * @param errorMessage the error message to classify
     * @return the classified reason: "timeout", "oom", "access_denied", or "unknown"
     */
    public static String classifyFailReason(String errorMessage) {
        if (Strings.isNullOrEmpty(errorMessage)) {
            return REASON_UNKNOWN;
        }
        String normalized = errorMessage.toLowerCase();
        if (normalized.contains("timeout") || normalized.contains("timed out")) {
            return REASON_TIMEOUT;
        }
        if (normalized.contains("outofmemory") || normalized.contains("out of memory")) {
            return REASON_OOM;
        }
        if (normalized.contains("access denied") || normalized.contains("permission denied")
                || normalized.contains("not authorized") || normalized.contains("unauthorized")) {
            return REASON_ACCESS_DENIED;
        }
        return REASON_UNKNOWN;
    }

    private static String normalizeCompactionType(String compactionType) {
        if (COMPACTION_TYPE_MANUAL.equalsIgnoreCase(compactionType)) {
            return COMPACTION_TYPE_MANUAL;
        }
        if (COMPACTION_TYPE_AUTO.equalsIgnoreCase(compactionType)) {
            return COMPACTION_TYPE_AUTO;
        }
        return Strings.isNullOrEmpty(compactionType) ? REASON_UNKNOWN : compactionType.toLowerCase();
    }

    // compaction (rewrite_data_files)
    public static void increaseIcebergCompactionTotal(String status, String reason, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        String normalizedStatus = normalizeStatus(status);
        String normalizedReason = normalizeReason(reason);
        String key = normalizedType + "|" + normalizedStatus + "|" + normalizedReason;
        LongCounterMetric counter = COUNTER_ICEBERG_COMPACTION_TOTAL.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_compaction_total",
                    Metric.MetricUnit.REQUESTS,
                    "total iceberg compaction tasks by type, status and reason");
            metric.addLabel(new MetricLabel("compaction_type", normalizedType));
            metric.addLabel(new MetricLabel("status", normalizedStatus));
            metric.addLabel(new MetricLabel("reason", normalizedReason));
            MetricRepo.addMetric(metric);
            return metric;
        });
        counter.increase(1L);
    }

    public static void increaseIcebergCompactionTotalSuccess() {
        increaseIcebergCompactionTotal(STATUS_SUCCESS, REASON_NONE, COMPACTION_TYPE_MANUAL);
    }

    public static void increaseIcebergCompactionTotalFail(Throwable t) {
        increaseIcebergCompactionTotal(STATUS_FAILED, classifyFailReason(t), COMPACTION_TYPE_MANUAL);
    }

    public static void increaseIcebergCompactionDurationMs(long durationMs, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        LongCounterMetric counter = COUNTER_ICEBERG_COMPACTION_DURATION_MS.computeIfAbsent(
                normalizedType, k -> {
                    LongCounterMetric metric = new LongCounterMetric("iceberg_compaction_duration_ms_total",
                            Metric.MetricUnit.MILLISECONDS, "total duration of iceberg compaction tasks");
                    metric.addLabel(new MetricLabel("compaction_type", normalizedType));
                    MetricRepo.addMetric(metric);
                    return metric;
                });
        counter.increase(durationMs);
    }

    public static void increaseIcebergCompactionInputFiles(long count, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        LongCounterMetric counter = COUNTER_ICEBERG_COMPACTION_INPUT_FILES.computeIfAbsent(
                normalizedType, k -> {
                    LongCounterMetric metric = new LongCounterMetric("iceberg_compaction_input_files_total",
                            Metric.MetricUnit.REQUESTS,
                            "total input data files for iceberg compaction");
                    metric.addLabel(new MetricLabel("compaction_type", normalizedType));
                    MetricRepo.addMetric(metric);
                    return metric;
                });
        counter.increase(count);
    }

    public static void increaseIcebergCompactionOutputFiles(long count, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        LongCounterMetric counter = COUNTER_ICEBERG_COMPACTION_OUTPUT_FILES.computeIfAbsent(
                normalizedType, k -> {
                    LongCounterMetric metric = new LongCounterMetric("iceberg_compaction_output_files_total",
                            Metric.MetricUnit.REQUESTS,
                            "total output data files generated by iceberg compaction");
                    metric.addLabel(new MetricLabel("compaction_type", normalizedType));
                    MetricRepo.addMetric(metric);
                    return metric;
                });
        counter.increase(count);
    }

    public static void increaseIcebergCompactionRemovedDeleteFiles(long count, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        LongCounterMetric counter = COUNTER_ICEBERG_COMPACTION_REMOVED_DELETE_FILES.computeIfAbsent(
                normalizedType, k -> {
                    LongCounterMetric metric = new LongCounterMetric("iceberg_compaction_removed_delete_files_total",
                            Metric.MetricUnit.REQUESTS,
                            "total delete files removed during iceberg compaction");
                    metric.addLabel(new MetricLabel("compaction_type", normalizedType));
                    MetricRepo.addMetric(metric);
                    return metric;
                });
        counter.increase(count);
    }
}
