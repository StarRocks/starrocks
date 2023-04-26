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

import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.parser.NodePosition;

public class RefreshSchemeDesc implements ParseNode {

    protected MaterializedView.RefreshType type;
    protected MaterializedView.RefreshMoment moment;
    protected final NodePosition pos;

    public RefreshSchemeDesc(MaterializedView.RefreshType type) {
        this(type, NodePosition.ZERO);
    }

    public RefreshSchemeDesc(MaterializedView.RefreshType type, NodePosition pos) {
        this(type, pos, MaterializedView.RefreshMoment.IMMEDIATE);
    }

    public RefreshSchemeDesc(MaterializedView.RefreshType type, NodePosition pos, MaterializedView.RefreshMoment moment) {
        this.type = type;
        this.moment = moment;
        this.pos = pos;
    }

    public MaterializedView.RefreshType getType() {
        return type;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public MaterializedView.RefreshMoment getMoment() {
        return moment;
    }

}
