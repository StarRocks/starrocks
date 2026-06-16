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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;

/**
 * Strategy for one INSERT pre-split source kind — INSERT-from-FILES vs
 * INSERT-from-OLAP-table. The two kinds share the same skeleton flow: the
 * skeleton owns the statement-shape pre-filters, target resolve + authorization,
 * and the conservative-skip eligibility gates, while {@link PreSplitFlow} owns
 * the submit flow that turns a resolved bundle into a reshard.
 *
 * <p>Each source supplies only the parts that differ: the per-path config flag,
 * the {@link LoadKind} it drives, the cheap mutually-exclusive statement-shape
 * {@link #matches match}, and the source resolve + gating that yields a
 * {@link PreSplitFlow.Prepared} bundle.
 */
interface InsertPreSplitSource {

    /** Whether this source's per-path Config flag enables pre-split cluster-wide. */
    boolean configEnabled();

    /** The load kind this source drives. */
    LoadKind loadKind();

    /**
     * Cheap statement-shape match. Implementations must be mutually exclusive so
     * a given INSERT shape selects at most one source.
     */
    boolean matches(InsertStmt insertStmt, SelectRelation selectRelation);

    /**
     * Resolves the source, runs the source-specific authorization / policy /
     * predicate gates, and builds the {@link PreSplitFlow.Prepared} bundle the
     * shared flow submits.
     *
     * <p>Precondition: {@link #matches} returned {@code true} for the same
     * {@code (insertStmt, selectRelation)} — implementations may cast the FROM
     * relation to their expected type.
     *
     * @return the prepared bundle, or {@code null} when any gate rejects (caller
     *         no-ops; pre-split is skipped).
     */
    PreSplitFlow.Prepared prepare(InsertStmt insertStmt, SelectRelation selectRelation,
                                  OlapTable target, Database database, ConnectContext context)
            throws AccessDeniedException;
}
