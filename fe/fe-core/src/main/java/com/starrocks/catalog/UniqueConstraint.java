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


package com.starrocks.catalog;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import org.spark_project.guava.base.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// unique constraint is used to guide optimizer rewrite for now,
// and is not enforced during ingestion.
// the unique property of data should be guaranteed by user.
//
// a table may have multi unique constraints.
public class UniqueConstraint {
    // here id is preferred, but meta of column does not have id.
    // have to use name here, so column rename is not supported
    private final List<String> uniqueColumns;

    public UniqueConstraint(List<String> uniqueColumns) {
        this.uniqueColumns = uniqueColumns;
    }

    public List<String> getUniqueColumns() {
        return uniqueColumns;
    }

    public String toString() {
        return Joiner.on(",").join(uniqueColumns);
    }

    public static List<UniqueConstraint> parse(String constraintDescs) {
        if (Strings.isNullOrEmpty(constraintDescs)) {
            return null;
        }
        String[] constraintArray = constraintDescs.split(";");
        List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
        for (String constraintDesc : constraintArray) {
            if (Strings.isNullOrEmpty(constraintDesc)) {
                continue;
            }
            String[] uniqueColumns = constraintDesc.split(",");
            List<String> columnNames =
                    Arrays.asList(uniqueColumns).stream().map(String::trim).collect(Collectors.toList());
            uniqueConstraints.add(new UniqueConstraint(columnNames));
        }
        return uniqueConstraints;
    }
}
