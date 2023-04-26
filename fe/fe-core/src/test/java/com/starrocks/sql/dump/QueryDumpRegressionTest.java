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

package com.starrocks.sql.dump;

import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class QueryDumpRegressionTest extends ReplayDumpTestBase {
    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    public void run(String name, QueryDumpCase queryDumpCase) throws Exception {
        String path = System.getProperty("dumpJsonConfig");
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        TExplainLevel explainLevel = null;
        if (queryDumpCase.getType() != null) {
            switch (queryDumpCase.getType()) {
                case "normal":
                    explainLevel = TExplainLevel.NORMAL;
                    break;
                case "costs":
                    explainLevel = TExplainLevel.COSTS;
                    break;
                case "verbose":
                    explainLevel = TExplainLevel.VERBOSE;
                    break;
                default:
            }
        }

        String replayPair = getPlanFragment(getDumpInfoFromFile(path + "/" + queryDumpCase.getName()),
                connectContext, explainLevel);
        String explain = getExplainFromFile(path + "/" + queryDumpCase.getName());
        Assertions.assertEquals(explain, replayPair);
    }

    private static Stream<Arguments> testCases() {
        String path = System.getProperty("dumpJsonConfig");
        QueryDumpRegressionTesting queryDumpRegressionTesting  = getCaseFromJsonConfig(path);
        List<Arguments> arguments = new ArrayList<>();
        for (QueryDumpCase queryDumpCase : queryDumpRegressionTesting.getCases()) {
            arguments.add(Arguments.of(queryDumpCase.getName(), queryDumpCase));
        }
        return arguments.stream();
    }
}
