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


package com.starrocks.sql.ast;

<<<<<<< HEAD
import com.google.common.collect.Maps;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

<<<<<<< HEAD
import java.util.Map;

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
// Alter clause.
public abstract class AlterClause implements ParseNode {

    protected AlterOpType opType;

    protected final NodePosition pos;

    protected AlterClause(AlterOpType opType, NodePosition pos) {
        this.pos = pos;
        this.opType = opType;
    }

<<<<<<< HEAD
    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public AlterOpType getOpType() {
        return opType;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
