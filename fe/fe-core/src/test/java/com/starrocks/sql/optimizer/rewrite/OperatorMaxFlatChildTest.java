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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class OperatorMaxFlatChildTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test(expected = SemanticException.class)
    public void testMaxCaseWhenChildren() throws Exception {
        final int prev = Config.max_scalar_operator_flat_children;
        Config.max_scalar_operator_flat_children = 10;
        try {
            String sql;
            sql = "select\n" +
                    "    case\n" +
                    "        when cw1 = \"X11\" then concat(cw1, \"11\")\n" +
                    "        when cw1 = \"X111\" then concat(cw1, \"111\")\n" +
                    "        when cw1 = \"X1111\" then concat(cw1, \"1111\")\n" +
                    "    end cw1\n" +
                    "from\n" +
                    "    (\n" +
                    "        select\n" +
                    "            case\n" +
                    "                when cw1 = \"X11\" then concat(cw1, \"11\")\n" +
                    "                when cw1 = \"X111\" then concat(cw1, \"111\")\n" +
                    "                when cw1 = \"X1111\" then concat(cw1, \"1111\")\n" +
                    "            end cw1\n" +
                    "        from\n" +
                    "            (\n" +
                    "                select\n" +
                    "                    case\n" +
                    "                        when cw1 = \"X2\" then concat(cw1, \"1\")\n" +
                    "                        when cw1 = \"X1\" then concat(cw1, \"11\")\n" +
                    "                        when cw1 = \"X3\" then concat(cw1, \"11\")\n" +
                    "                    end cw1\n" +
                    "                from\n" +
                    "                    (\n" +
                    "                        select\n" +
                    "                            case\n" +
                    "                                when cw1 = 1 then upper(cw1)\n" +
                    "                                when cw1 = 2 then cw1\n" +
                    "                                when cw1 = 3 then lower(cw1)\n" +
                    "                            end cw1\n" +
                    "                        from\n" +
                    "                            (\n" +
                    "                                select\n" +
                    "                                    case\n" +
                    "                                        when t1a = 1 then t1a\n" +
                    "                                        when t1a = 2 then t1b\n" +
                    "                                        when t1a = 3 then t1c\n" +
                    "                                    end cw1\n" +
                    "                                from\n" +
                    "                                    test_all_type\n" +
                    "                            ) t\n" +
                    "                    ) t\n" +
                    "            ) t\n" +
                    "    ) t;\n";
            getFragmentPlan(sql);
        } finally {
            Config.max_scalar_operator_flat_children = prev;
        }
    }

}
