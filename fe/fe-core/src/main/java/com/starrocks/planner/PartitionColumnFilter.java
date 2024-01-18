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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/PartitionColumnFilter.java

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

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PartitionColumnFilter {
    private static final Logger LOG = LogManager.getLogger(PartitionColumnFilter.class);
    private LiteralExpr lowerBound;
    private LiteralExpr upperBound;
    public boolean lowerBoundInclusive;
    public boolean upperBoundInclusive;
    // InPredicate
    // planner and TestUT use
    private InPredicate inPredicate;

    // fact use
    private List<LiteralExpr> inPredicateLiterals;

    public InPredicate getInPredicate() {
        return inPredicate;
    }

    public void setInPredicate(InPredicate inPredicate) {
        this.inPredicate = inPredicate;
        this.inPredicateLiterals = Lists.newArrayList();
        for (int i = 1; i < inPredicate.getChildren().size(); i++) {
            inPredicateLiterals.add((LiteralExpr) inPredicate.getChild(i));
        }
    }

    public List<LiteralExpr> getInPredicateLiterals() {
        return inPredicateLiterals;
    }

    public void setInPredicateLiterals(List<LiteralExpr> inPredicateLiterals) {
        this.inPredicateLiterals = inPredicateLiterals;
    }

    // select the bigger bound
    public void setLowerBound(LiteralExpr newLowerBound, boolean newLowerBoundInclusive) {
        if (null == lowerBound) {
            lowerBound = newLowerBound;
            lowerBoundInclusive = newLowerBoundInclusive;
        } else {
            int ret = lowerBound.compareLiteral(newLowerBound);
            if (ret < 0) {
                lowerBound = newLowerBound;
                lowerBoundInclusive = newLowerBoundInclusive;
            } else if (ret == 0) {
                if (newLowerBoundInclusive == false) {
                    lowerBoundInclusive = newLowerBoundInclusive;
                }
            } else {
                // pass
            }
        }
    }

    // select the smaller bound
    public void setUpperBound(LiteralExpr newUpperBound, boolean newUpperBoundInclusive) {
        if (null == upperBound) {
            upperBound = newUpperBound;
            upperBoundInclusive = newUpperBoundInclusive;
        } else {
            int ret = upperBound.compareLiteral(newUpperBound);
            if (ret < 0) {
                // pass
            } else if (ret == 0) {
                if (newUpperBoundInclusive == false) {
                    upperBoundInclusive = newUpperBoundInclusive;
                }
            } else {
                upperBound = newUpperBound;
                upperBoundInclusive = newUpperBoundInclusive;
            }
        }
    }

    public LiteralExpr getLowerBound() {
        return lowerBound;
    }

    public LiteralExpr getUpperBound() {
        return upperBound;
    }

    public LiteralExpr getLowerBound(boolean isConvertToDate) throws SemanticException {
        if (isConvertToDate) {
            return PartitionUtil.convertToDateLiteral(lowerBound);
        } else {
            return lowerBound;
        }
    }

    public LiteralExpr getUpperBound(boolean isConvertToDate) throws SemanticException {
        if (isConvertToDate) {
            return PartitionUtil.convertToDateLiteral(upperBound);
        } else {
            return upperBound;
        }
    }

    public Range<PartitionKey> getRange(List<Column> columns) {
        LOG.info("range is " + toString());
        BoundType lowerType = lowerBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
        BoundType upperType = upperBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
        if (null != lowerBound && null != upperBound) {
            PartitionKey lowerKey = null;
            PartitionKey upperKey = null;
            // cmy mod, catch AnalysisException
            try {
                lowerKey = PartitionKey.createPartitionKey(
                        Lists.newArrayList(new PartitionValue(lowerBound.getStringValue())), columns);
                upperKey = PartitionKey.createPartitionKey(
                        Lists.newArrayList(new PartitionValue(upperBound.getStringValue())), columns);
            } catch (AnalysisException e) {
                LOG.warn(e.getMessage());
                return null;
            }
            return Range.range(lowerKey, lowerType, upperKey, upperType);
        }
        return null;
    }

    public Type getFilterType() {
        if (lowerBound != null) {
            return lowerBound.getType();
        }
        if (upperBound != null) {
            return upperBound.getType();
        }
        if (inPredicate != null) {
            return inPredicate.getChild(0).getType();
        }
        if (inPredicateLiterals != null && !inPredicateLiterals.isEmpty()) {
            return inPredicateLiterals.get(0).getType();
        }
        return null;
    }

    public boolean isPoint() {
        return lowerBoundInclusive && upperBoundInclusive && lowerBound != null && lowerBound.equals(upperBound);
    }

    @Override
    public String toString() {
        String str = "";
        if (null == lowerBound) {
            str += "lowerBound is UNSET";
        } else {
            str += "lowerBound is " + lowerBound.getStringValue() + " and lowerBoundInclusive is " +
                    lowerBoundInclusive;
        }
        if (null == upperBound) {
            str += "\nupperBound is UNSET";
        } else {
            str += "\nupperBound is " + upperBound.getStringValue() + " and upperBoundInclusive is " +
                    upperBoundInclusive;
        }
        if (null == inPredicate) {
            str += "\ninPredicate is UNSET";
        } else {
            str += "\ninPredicate is " + inPredicate;
        }
        if (null == inPredicateLiterals || inPredicateLiterals.isEmpty()) {
            str += "\ninPredicateLiterals is UNSET";
        } else {
            str += "\ninPredicateLiterals is " + inPredicateLiterals;
        }
        return str;
    }
}
