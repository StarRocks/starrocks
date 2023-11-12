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

package com.starrocks.analysis;

import com.starrocks.analysis.MatchPredicate.MatchBinaryPredicate;
import com.starrocks.analysis.MatchPredicate.MatchTriplePredicate;
import com.starrocks.analysis.MatchPredicate.Operator;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class MatchPredicateTest {

    @Test
    public void testMatchPredicate() throws AnalysisException {
        MatchBinaryPredicate matchTermPredicate =
                new MatchBinaryPredicate(new SlotRef(null, "k1"), StringLiteral.create("hello", Type.VARCHAR),
                        Operator.MATCH_TERM);
        MatchBinaryPredicate matchAllPredicate =
                new MatchBinaryPredicate(new SlotRef(null, "hello kitty"), StringLiteral.create("1", Type.VARCHAR),
                        Operator.MATCH_ALL);
        MatchBinaryPredicate matchWildcardPredicate =
                new MatchBinaryPredicate(new SlotRef(null, "k1"), StringLiteral.create("*ello*", Type.VARCHAR),
                        Operator.MATCH_WILDCARD);

        MatchTriplePredicate matchFuzzyPredicate =
                new MatchTriplePredicate(new SlotRef(null, "k1"), StringLiteral.create("hello", Type.VARCHAR),
                        IntLiteral.create("1", Type.INT),
                        Operator.MATCH_FUZZY);

        MatchTriplePredicate matchPhrasePredicate =
                new MatchTriplePredicate(new SlotRef(null, "k1"), StringLiteral.create("hello world", Type.VARCHAR),
                        IntLiteral.create("2", Type.INT),
                        Operator.MATCH_PHRASE);

        Assertions.assertEquals(matchTermPredicate.clone(), matchTermPredicate);
        Assertions.assertEquals(matchAllPredicate.clone(), matchAllPredicate);
        Assertions.assertEquals(matchWildcardPredicate.clone(), matchWildcardPredicate);
        Assertions.assertEquals(matchFuzzyPredicate.clone(), matchFuzzyPredicate);
        Assertions.assertEquals(matchPhrasePredicate.clone(), matchPhrasePredicate);

        Assertions.assertEquals(matchPhrasePredicate.toSqlImpl(), "MATCH_PHRASE(`k1`,'hello world',2)");
    }
}
