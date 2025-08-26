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

package com.starrocks.sql.optimizer.rule.tvr.common;

import com.starrocks.common.tvr.TvrTableDeltaTrait;

/**
 * TvrOptMeta is the metadata of TvrOptExpression that represents a range of TVR versions.
 */
public record TvrOptMeta(TvrTableDeltaTrait tvrDeltaTrait, TvrLazyOptExpression from, TvrLazyOptExpression to) {
    public static final TvrOptMeta UNSUPPORTED = new TvrOptMeta(
            TvrTableDeltaTrait.DEFAULT,
            TvrLazyOptExpression.of(() -> {
                throw new IllegalStateException("TvrOptExpression for delta aggregate is not " +
                        "supported");
            }),
            TvrLazyOptExpression.of(() -> {
                throw new IllegalStateException("TvrOptExpression for delta aggregate is not " +
                        "supported");
            })
    );

    public TvrLazyOptExpression mapFrom(TvrOptApplier tvrOptApplier) {
        return from.map(tvrOptApplier);
    }

    public TvrLazyOptExpression mapTo(TvrOptApplier tvrOptApplier) {
        return to.map(tvrOptApplier);
    }

    public static TvrOptMeta mapChild(TvrOptMeta child, TvrOptApplier tvrOptApplier) {
        return new TvrOptMeta(child.tvrDeltaTrait(), child.mapFrom(tvrOptApplier), child.mapTo(tvrOptApplier));
    }

    public TvrOptExpression getFrom() {
        return from.get();
    }

    public TvrOptExpression getTo() {
        return to.get();
    }

    public boolean isAppendOnly() {
        return tvrDeltaTrait().isAppendOnly();
    }
}
