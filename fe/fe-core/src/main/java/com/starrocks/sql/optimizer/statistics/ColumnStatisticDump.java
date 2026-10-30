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

package com.starrocks.sql.optimizer.statistics;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

/**
 * Serialization form of {@link ColumnStatistic} used by query dumps.
 */
public class ColumnStatisticDump {
    public static final int CURRENT_VERSION = 1;

    @SerializedName("version")
    public int version = CURRENT_VERSION;

    @SerializedName("min")
    public String minValue;

    @SerializedName("max")
    public String maxValue;

    @SerializedName("nullsFraction")
    public String nullsFraction;

    @SerializedName("averageRowSize")
    public String averageRowSize;

    @SerializedName("distinctValuesCount")
    public String distinctValuesCount;

    @SerializedName("collectionSize")
    public String collectionSize;

    @SerializedName("type")
    public String type;

    @SerializedName("minString")
    public String minString;

    @SerializedName("maxString")
    public String maxString;

    @SerializedName("histogram")
    public HistogramDump histogram;

    public static ColumnStatisticDump from(ColumnStatistic statistic) {
        ColumnStatisticDump dump = new ColumnStatisticDump();
        dump.version = CURRENT_VERSION;
        dump.minValue = Double.toString(statistic.getMinValue());
        dump.maxValue = Double.toString(statistic.getMaxValue());
        dump.nullsFraction = Double.toString(statistic.getNullsFraction());
        dump.averageRowSize = Double.toString(statistic.getAverageRowSize());
        dump.distinctValuesCount = Double.toString(statistic.getDistinctValuesCount());
        dump.collectionSize = Double.toString(statistic.getCollectionSize());
        dump.type = statistic.getType().name();
        dump.minString = statistic.getMinString();
        dump.maxString = statistic.getMaxString();
        if (statistic.getHistogram() != null) {
            dump.histogram = HistogramDump.from(statistic.getHistogram());
        }
        return dump;
    }

    public ColumnStatistic toColumnStatistic() {
        ColumnStatistic.Builder builder = ColumnStatistic.builder()
                .setMinValue(parseDouble(minValue, NEGATIVE_INFINITY))
                .setMaxValue(parseDouble(maxValue, POSITIVE_INFINITY))
                .setNullsFraction(parseDouble(nullsFraction, 0))
                .setAverageRowSize(parseDouble(averageRowSize, 1))
                .setDistinctValuesCount(parseDouble(distinctValuesCount, 1))
                .setCollectionSize(parseDouble(collectionSize, ColumnStatistic.DEFAULT_COLLECTION_SIZE))
                .setType(parseType(type))
                .setMinString(minString)
                .setMaxString(maxString);
        if (histogram != null) {
            builder.setHistogram(histogram.toHistogram());
        }
        return builder.build();
    }

    private static double parseDouble(String value, double defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Double.parseDouble(value);
    }

    private static ColumnStatistic.StatisticType parseType(String value) {
        if (value == null || value.isEmpty()) {
            return ColumnStatistic.StatisticType.ESTIMATE;
        }
        return ColumnStatistic.StatisticType.valueOf(value);
    }

    /**
     * Serialization form of {@link Histogram}. Persists the complete MCV map and every bucket.
     */
    public static class HistogramDump {
        @SerializedName("buckets")
        public List<BucketDump> buckets;

        @SerializedName("mcv")
        public Map<String, Long> mcv;

        public static HistogramDump from(Histogram histogram) {
            HistogramDump dump = new HistogramDump();
            if (histogram.getBuckets() != null) {
                dump.buckets = new ArrayList<>();
                for (Bucket bucket : histogram.getBuckets()) {
                    dump.buckets.add(BucketDump.from(bucket));
                }
            }
            dump.mcv = histogram.getMCV();
            return dump;
        }

        public Histogram toHistogram() {
            List<Bucket> bucketList = new ArrayList<>();
            if (buckets != null) {
                for (BucketDump bucketDump : buckets) {
                    bucketList.add(bucketDump.toBucket());
                }
            }
            return new Histogram(bucketList, mcv);
        }
    }

    /**
     * Serialization form of {@link Bucket}.
     */
    public static class BucketDump {
        @SerializedName("lower")
        public String lower;

        @SerializedName("upper")
        public String upper;

        @SerializedName("count")
        public Long count;

        @SerializedName("upperRepeats")
        public Long upperRepeats;

        // nullable - only present when the source bucket carries a per-bucket distinct count
        @SerializedName("distinctCount")
        public Long distinctCount;

        public static BucketDump from(Bucket bucket) {
            BucketDump dump = new BucketDump();
            dump.lower = Double.toString(bucket.getLower());
            dump.upper = Double.toString(bucket.getUpper());
            dump.count = bucket.getCount();
            dump.upperRepeats = bucket.getUpperRepeats();
            bucket.getDistinctCount().ifPresent(value -> dump.distinctCount = value);
            return dump;
        }

        public Bucket toBucket() {
            double lowerValue = parseDouble(lower, 0);
            double upperValue = parseDouble(upper, 0);
            if (distinctCount != null) {
                return new Bucket(lowerValue, upperValue, count, upperRepeats, distinctCount);
            }
            return new Bucket(lowerValue, upperValue, count, upperRepeats);
        }
    }
}

