// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/Planner.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.thrift.TExplainLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class Planner {
    private static final Logger LOG = LogManager.getLogger(Planner.class);

    private boolean isBlockQuery = false;

    private ArrayList<PlanFragment> fragments = Lists.newArrayList();

    public boolean isBlockQuery() {
        return isBlockQuery;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    /**
     * Return combined explain string for all plan fragments.
     */
    public String getExplainString(List<PlanFragment> fragments, TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            if (i > 0) {
                // a blank line between plan fragments
                str.append("\n");
            }
            if (explainLevel.equals(TExplainLevel.NORMAL)) {
                str.append("PLAN FRAGMENT ").append(i).append("\n");
                str.append(fragment.getExplainString(TExplainLevel.NORMAL));
            } else {
                str.append("PLAN FRAGMENT ").append(i).append("(").append(fragment.getFragmentId()).append(")\n");
                str.append(fragment.getVerboseExplain());
            }
        }
        return str.toString();
    }
}
