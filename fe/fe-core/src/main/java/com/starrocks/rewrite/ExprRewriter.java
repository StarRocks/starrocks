// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/ExprRewriter.java

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

package com.starrocks.rewrite;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.common.AnalysisException;

import java.util.List;

/**
 * Helper class that drives the transformation of Exprs according to a given list of
 * ExprRewriteRules. The rules are applied as follows:
 * - a single rule is applied repeatedly to the Expr and all its children in a bottom-up
 * fashion until there are no more changes
 * - the rule list is applied repeatedly until no rule has made any changes
 * - the rules are applied in the order they appear in the rule list
 * Keeps track of how many transformations were applied.
 */
@Deprecated
public class ExprRewriter {
    private int numChanges = 0;

    public Expr rewrite(Expr expr, Analyzer analyzer) throws AnalysisException {
        return expr;
    }

    public void rewriteList(List<Expr> exprs, Analyzer analyzer) throws AnalysisException {
        for (int i = 0; i < exprs.size(); ++i) {
            exprs.set(i, rewrite(exprs.get(i), analyzer));
        }
    }

    public void reset() {
        numChanges = 0;
    }

    public boolean changed() {
        return numChanges > 0;
    }
}
