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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class HistogramUtils {
    private static final Logger LOG = LogManager.getLogger(HistogramUtils.class);

    public static List<Bucket> convertBuckets(String histogramString, Type type) throws AnalysisException {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();

        JsonElement jsonElement = jsonObject.get("buckets");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyList();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            try {
                double low;
                double high;
                if (type.isDate()) {
                    low = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                            bucketJsonArray.get(0).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
                    high = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                            bucketJsonArray.get(1).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
                } else if (type.isDatetime()) {
                    low = (double) getLongFromDateTime(DateUtils.parseDatTimeString(
                            bucketJsonArray.get(0).getAsString()));
                    high = (double) getLongFromDateTime(DateUtils.parseDatTimeString(
                            bucketJsonArray.get(1).getAsString()));
                } else {
                    low = Double.parseDouble(bucketJsonArray.get(0).getAsString());
                    high = Double.parseDouble(bucketJsonArray.get(1).getAsString());
                }

                if (bucketJsonArray.size() == 5) {
                    Bucket bucket = new Bucket(low, high,
                            Long.parseLong(bucketJsonArray.get(2).getAsString()),
                            Long.parseLong(bucketJsonArray.get(3).getAsString()),
                            Long.parseLong(bucketJsonArray.get(4).getAsString()));
                    buckets.add(bucket);
                } else {
                    Bucket bucket = new Bucket(low, high,
                            Long.parseLong(bucketJsonArray.get(2).getAsString()),
                            Long.parseLong(bucketJsonArray.get(3).getAsString()));
                    buckets.add(bucket);
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse histogram bucket: {}", bucketJsonArray, e);
            }
        }
        return buckets;
    }

    public static Map<String, Long> convertMCV(String histogramString) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();
        JsonElement jsonElement = jsonObject.get("mcv");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyMap();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        Map<String, Long> mcv = new HashMap<>();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            mcv.put(bucketJsonArray.get(0).getAsString(), Long.parseLong(bucketJsonArray.get(1).getAsString()));
        }
        return mcv;
    }

    // Serialize to the BE-style {"buckets":[...],"mcv":[...]} JSON; bounds kept as raw double (type-agnostic).
    public static String serializeHistogram(Histogram histogram) {
        JsonObject root = new JsonObject();

        JsonArray bucketsArray = new JsonArray();
        if (histogram.getBuckets() != null) {
            for (Bucket bucket : histogram.getBuckets()) {
                JsonArray bucketArray = new JsonArray();
                bucketArray.add(Double.toString(bucket.getLower()));
                bucketArray.add(Double.toString(bucket.getUpper()));
                bucketArray.add(Long.toString(bucket.getCount()));
                bucketArray.add(Long.toString(bucket.getUpperRepeats()));
                bucket.getDistinctCount().ifPresent(distinctCount -> bucketArray.add(Long.toString(distinctCount)));
                bucketsArray.add(bucketArray);
            }
        }
        root.add("buckets", bucketsArray);

        JsonArray mcvArray = new JsonArray();
        if (histogram.getMCV() != null) {
            for (Map.Entry<String, Long> entry : histogram.getMCV().entrySet()) {
                JsonArray mcvEntry = new JsonArray();
                mcvEntry.add(entry.getKey());
                mcvEntry.add(Long.toString(entry.getValue()));
                mcvArray.add(mcvEntry);
            }
        }
        root.add("mcv", mcvArray);

        return root.toString();
    }

    // Inverse of serializeHistogram; bounds parsed straight back to double, no column Type needed.
    public static Histogram deserializeHistogram(String histogramString) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();

        List<Bucket> buckets = Lists.newArrayList();
        JsonElement bucketsElement = jsonObject.get("buckets");
        if (bucketsElement != null && !bucketsElement.isJsonNull()) {
            JsonArray bucketsArray = bucketsElement.getAsJsonArray();
            for (int i = 0; i < bucketsArray.size(); ++i) {
                JsonArray bucketArray = bucketsArray.get(i).getAsJsonArray();
                double lower = Double.parseDouble(bucketArray.get(0).getAsString());
                double upper = Double.parseDouble(bucketArray.get(1).getAsString());
                long count = Long.parseLong(bucketArray.get(2).getAsString());
                long upperRepeats = Long.parseLong(bucketArray.get(3).getAsString());
                if (bucketArray.size() == 5) {
                    buckets.add(new Bucket(lower, upper, count, upperRepeats,
                            Long.parseLong(bucketArray.get(4).getAsString())));
                } else {
                    buckets.add(new Bucket(lower, upper, count, upperRepeats));
                }
            }
        }

        return new Histogram(buckets, convertMCV(histogramString));
    }
}
