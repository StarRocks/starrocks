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

import com.google.common.base.Preconditions;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Util;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class PieceAuxState {
    private int id = -1;
    private PlanPiece parent = null;

    private PrettyPrinter norm = null;

    private transient String normHash = null;

    private Set<Integer> colocateBucketKey = null;

    public PieceAuxState() {
    }

    private PieceAuxState(PieceAuxState auxState) {
        this.id = auxState.id;
        this.parent = auxState.parent;
        this.norm = auxState.norm;
    }

    public Optional<Set<Integer>> getColocateBucketKey() {
        return Optional.ofNullable(colocateBucketKey);
    }

    public void setColocateBucketKey(Set<Integer> colocateBucketKey) {
        Preconditions.checkState(this.colocateBucketKey == null);
        this.colocateBucketKey = Objects.requireNonNull(colocateBucketKey);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public PlanPiece getParent() {
        return parent;
    }

    public void setParent(PlanPiece parent) {
        this.parent = parent;
    }

    public PrettyPrinter getNorm() {
        return norm;
    }

    public void setNorm(PrettyPrinter norm) {
        this.norm = Objects.requireNonNull(norm);
    }

    public String getNormHash() {
        if (normHash == null) {
            normHash = Util.md5(norm.getResult());
        }
        return normHash;
    }

    public PieceAuxState duplicate() {
        return new PieceAuxState(this);
    }
}
