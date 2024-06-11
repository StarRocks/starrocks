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

package com.starrocks.sql.automv.pn;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.util.EitherOr;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

// OpPlus2 is used to rewrite aggregation function, it represents
// rewrite result. for an example:
// rewrite: OpPlus->OpPlus2
// After rewriting, OpPlus2.op is equivalent to the input OpPlus but it is
// expressed using OpPlus2.args, OpPlus2.args are dependent aggregation functions
// if the dependent function is already-existing, then we put it in EitherOr.first,
// otherwise, it is a new-created dependent aggregation function, so we put it in
// EitherOr.second, we use List<EitherOr<OpPlus>> as the data structure to avoid
// create duplicate aggregation functions.
public class OpPlus2 {
    private final OpPlus op;
    private final List<EitherOr<OpPlus>> args;

    private OpPlus2(OpPlus op, List<EitherOr<OpPlus>> args) {
        this.op = Objects.requireNonNull(op);
        this.args = Objects.requireNonNull(args);
    }

    public static OpPlus2 of(OpPlus op, List<EitherOr<OpPlus>> args) {
        return new OpPlus2(op, args);
    }

    @SafeVarargs
    public static OpPlus2 of(OpPlus op, EitherOr<OpPlus>... args) {
        return of(op, ImmutableList.copyOf(args));
    }

    public OpPlus getOp() {
        return op;
    }

    public List<EitherOr<OpPlus>> getArgs() {
        return args;
    }

    public Stream<OpPlus> getNewArgs() {
        return args.stream().map(EitherOr::getSecond).flatMap(op -> op.map(Stream::of).orElseGet(Stream::empty));
    }

    public TieredMap<Integer, GenericColumn> getNewColumns() {
        return getNewArgs().collect(TieredMap.toMap(OpPlus::getId, opp -> GenericColumn.derived(opp.getOp())));
    }
}
