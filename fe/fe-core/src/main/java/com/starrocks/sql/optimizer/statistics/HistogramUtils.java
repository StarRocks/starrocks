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
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
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

                Bucket bucket = new Bucket(low, high,
                        Long.parseLong(bucketJsonArray.get(2).getAsString()),
                        Long.parseLong(bucketJsonArray.get(3).getAsString()));
                buckets.add(bucket);
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
}
