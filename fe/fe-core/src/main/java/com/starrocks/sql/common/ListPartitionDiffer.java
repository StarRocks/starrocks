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

package com.starrocks.sql.common;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public final class ListPartitionDiffer extends PartitionDiffer {
    private static final Logger LOG = LogManager.getLogger(ListPartitionDiffer.class);

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     *
     * @param srcListMap src partition list map
     * @param dstListMap dst partition list map
     * @return the different partition list map
     */
    public static Map<String, PListCell> diffList(Map<String, PListCell> srcListMap,
                                                  Map<String, PListCell> dstListMap) {

        Map<String, PListCell> result = Maps.newTreeMap();
        for (Map.Entry<String, PListCell> srcEntry : srcListMap.entrySet()) {
            String key = srcEntry.getKey();
            PListCell srcItem = srcEntry.getValue();
            if (srcItem.equals(dstListMap.get(key))) {
                continue;
            }
            result.put(key, srcEntry.getValue());
        }
        return result;
    }

    /**
     * Check if the partition of the base table and the partition of the mv have changed.
     *
     * @param baseListMap the partition name to its list partition cell of the base table
     * @param mvListMap   the partition name to its list partition cell of the mv
     * @return true if the partition has changed, otherwise false
     */
    public static boolean hasListPartitionChanged(Map<String, PListCell> baseListMap,
                                                  Map<String, PListCell> mvListMap) {
        if (checkListPartitionChanged(baseListMap, mvListMap)) {
            return true;
        }
        if (checkListPartitionChanged(mvListMap, baseListMap)) {
            return true;
        }
        return false;
    }

    /**
     * Check if src list map is different from dst list map.
     *
     * @param srcListMap src partition list map
     * @param dstListMap dst partition list map
     * @return true if the partition has changed, otherwise false
     */
    public static boolean checkListPartitionChanged(Map<String, PListCell> srcListMap,
                                                    Map<String, PListCell> dstListMap) {
        for (Map.Entry<String, PListCell> srcEntry : srcListMap.entrySet()) {
            String key = srcEntry.getKey();
            PListCell srcItem = srcEntry.getValue();
            if (!srcItem.equals(dstListMap.get(key))) {
                return true;
            }
        }
        return false;
    }
}
