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

/**
 * A table whose access Lake Formation decided. A type rather than a field, so a copy cannot lose it and every
 * guard covers every governed format. Column questions are case insensitive; which placeholder names count(*)
 * may scan stays the access controller's decision.
 */
public interface LakeFormationGovernedTable {

    /** Which table Lake Formation was asked about, for the message a refusal produces. */
    LakeFormationTableIdentity getLakeFormationIdentity();

    /** Whether Lake Formation granted this principal SELECT on the column. */
    boolean isColumnAuthorized(String columnName);

    /** From the physical lists, so an unauthorized real column is refused rather than taken for a placeholder. */
    boolean isPhysicalColumn(String columnName);
}
