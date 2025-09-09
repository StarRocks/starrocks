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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr.common;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * TvrLazyOptExpression is a wrapper for TvrOptExpression that allows lazy loading of the expression.
 * @param lazy
 */
public record TvrLazyOptExpression(Supplier<TvrOptExpression> lazy) {
    public TvrLazyOptExpression(Supplier<TvrOptExpression> lazy) {
        this.lazy = Suppliers.memoize(lazy);
    }

    public static TvrLazyOptExpression of(Supplier<TvrOptExpression> lazy) {
        return new TvrLazyOptExpression(lazy);
    }

    public TvrOptExpression get() {
        return lazy.get();
    }

    public TvrLazyOptExpression map(TvrOptApplier tvrOptApplier) {
        return TvrLazyOptExpression.of(() -> tvrOptApplier.apply(this.get()));
    }
}
