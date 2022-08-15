// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.common.util;

import com.starrocks.common.proc.BaseProcResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProcResultUtils {
    public static void convertToMetaResult(BaseProcResult result, List<List<Comparable>> infos) {
        // order by asc
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(infos, comparator);

        // set result
        for (List<Comparable> info : infos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(String.valueOf(comparable));
            }
            result.addRow(row);
        }
    }
}
