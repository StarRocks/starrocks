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

package com.starrocks.epack.connector.lakeformation;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.glue.model.ColumnRowFilter;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * What Lake Formation said about one table, taken straight off GetUnfilteredTableMetadata.
 *
 * Two response shapes are preserved rather than flattened, because flattening either one loses a
 * distinction that decides whether we may read data:
 *
 * - isRegisteredWithLakeFormation is a nullable Boolean in the API. Only an explicit false means
 *   "not registered, use the ordinary path"; an absent flag must be an error, never a silent no.
 * - hasAuthorizedColumns and an empty authorizedColumns list are different answers: the first says
 *   the API did not speak about columns, the second says it authorized none of them.
 */
public final class AuthorizedTableMetadata {
    private final Table table;
    private final Boolean registeredWithLakeFormation;
    private final boolean hasAuthorizedColumns;
    private final List<String> authorizedColumns;
    private final String rowFilter;
    private final List<ColumnRowFilter> cellFilters;
    private final String queryAuthorizationId;

    private AuthorizedTableMetadata(Table table, Boolean registeredWithLakeFormation,
                                    boolean hasAuthorizedColumns, List<String> authorizedColumns,
                                    String rowFilter, List<ColumnRowFilter> cellFilters,
                                    String queryAuthorizationId) {
        this.table = table;
        this.registeredWithLakeFormation = registeredWithLakeFormation;
        this.hasAuthorizedColumns = hasAuthorizedColumns;
        this.authorizedColumns = ImmutableList.copyOf(authorizedColumns);
        this.rowFilter = rowFilter;
        this.cellFilters = ImmutableList.copyOf(cellFilters);
        this.queryAuthorizationId = queryAuthorizationId;
    }

    public static AuthorizedTableMetadata from(GetUnfilteredTableMetadataResponse response) {
        requireNonNull(response, "response is null");
        return new AuthorizedTableMetadata(
                response.table(),
                response.isRegisteredWithLakeFormation(),
                response.hasAuthorizedColumns(),
                response.authorizedColumns(),
                response.rowFilter(),
                response.cellFilters(),
                response.queryAuthorizationId());
    }

    /** The Glue table as Lake Formation returned it, including its StorageDescriptor Location. */
    public Table table() {
        return table;
    }

    /**
     * @throws LakeFormationTableAccessException when the response carried no flag at all - guessing
     *         either way would either bypass Lake Formation or break an ordinary table
     */
    public boolean isRegistered(LakeFormationTableIdentity identity) {
        if (registeredWithLakeFormation == null) {
            throw new LakeFormationTableAccessException("Lake Formation returned no "
                    + "IsRegisteredWithLakeFormation flag for " + identity
                    + "; refusing to guess whether it is registered");
        }
        return registeredWithLakeFormation;
    }

    public boolean hasAuthorizedColumns() {
        return hasAuthorizedColumns;
    }

    public List<String> authorizedColumns() {
        return authorizedColumns;
    }

    public String rowFilter() {
        return rowFilter;
    }

    public List<ColumnRowFilter> cellFilters() {
        return cellFilters;
    }

    /** Passed back verbatim when vending credentials; never rebuilt or substituted. */
    public String queryAuthorizationId() {
        return queryAuthorizationId;
    }

    /**
     * Whether Lake Formation asked for row or cell filtering that this version cannot apply.
     *
     * Row and cell filters are not supported yet and must fail the query rather than be dropped
     * silently - but "a filter is present" is not the same as "a filter restricts anything". A
     * plain full-table SELECT grant with nothing configured comes back as
     * {@code rowFilter="TRUE"} plus one cell filter per authorized column, each with the
     * expression {@code TRUE}. Treating that as a restriction refuses every table, including the
     * ones nobody filtered.
     *
     * Only TRUE - in any case, trimmed - is recognised as "no restriction". Anything this code does
     * not understand, such as another tautology spelling like 1=1, a partial predicate, or an empty
     * string, counts as a restriction: dropping a filter we failed to parse is the unsafe direction.
     */
    public boolean hasFilters() {
        // An absent row filter means row filtering was never asked for.
        if (rowFilter != null && !isUnrestricted(rowFilter)) {
            return true;
        }
        for (ColumnRowFilter cellFilter : cellFilters) {
            // A cell filter entry that exists but carries no expression is a shape this code does
            // not understand, and an unparsed filter must not be dropped - so absent restricts here,
            // unlike the row filter above.
            if (!isUnrestricted(cellFilter.rowFilterExpression())) {
                return true;
            }
        }
        return false;
    }

    /** TRUE in any case and with surrounding space allows everything; every other expression restricts. */
    private static boolean isUnrestricted(String expression) {
        return expression != null && "TRUE".equalsIgnoreCase(expression.trim());
    }

    @Override
    public String toString() {
        return "AuthorizedTableMetadata{registered=" + registeredWithLakeFormation
                + ", hasAuthorizedColumns=" + hasAuthorizedColumns
                + ", authorizedColumnCount=" + authorizedColumns.size()
                + ", hasRowFilter=" + (rowFilter != null)
                + ", cellFilterCount=" + cellFilters.size()
                + ", queryAuthorizationId=<redacted>}";
    }
}
