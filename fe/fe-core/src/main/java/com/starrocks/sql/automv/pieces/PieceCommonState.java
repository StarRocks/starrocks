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

package com.starrocks.sql.automv.pieces;

import com.starrocks.sql.automv.column.ColumnRefToIdConverter;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PieceCommonState {
    private final ColumnRefToIdConverter idConverter;
    private final Map<String, FQTable> fqTableMap;

    private final Set<String> coveredQueries;

    public PieceCommonState(ColumnRefToIdConverter idConverter, Set<String> coveredQueries,
                            Map<String, FQTable> fqTableMap) {
        this.idConverter = Objects.requireNonNull(idConverter);
        this.coveredQueries = Objects.requireNonNull(coveredQueries);
        this.fqTableMap = Objects.requireNonNull(fqTableMap);
    }

    public ColumnRefToIdConverter getIdConverter() {
        return Objects.requireNonNull(idConverter);
    }

    public Map<String, FQTable> getFqTableMap() {
        return Objects.requireNonNull(fqTableMap);
    }

    public Set<String> getCoveredQueries() {
        return coveredQueries;
    }

    public PieceCommonState duplicate() {
        return new PieceCommonState(idConverter.duplicate(), coveredQueries, fqTableMap);
    }
}
