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

import com.starrocks.connector.exception.StarRocksConnectorException;

/**
 * A Lake Formation failure, unchecked so it can cross the connector SPI.
 *
 * It exists for its type, not for an error code: fail-closed paths have to recognize a Lake Formation
 * failure so it can punch through catch blocks that swallow generic connector exceptions.
 *
 * No dedicated ErrorCode on purpose. Authorization refusals get their MySQL error code the same way
 * Ranger's do - AccessDeniedException.reportAccessDenied sends anything denied by an
 * ExternalAccessController to ERR_ACCESS_DENIED_FOR_EXTERNAL_ACCESS_CONTROLLER - so the Lake Formation
 * access controller inherits that for free by extending ExternalAccessController. Everything else here
 * is a connector limitation or an internal ordering error, and surfaces as 1064 with a clear message,
 * exactly like every other connector limitation in the FE.
 */
public class LakeFormationTableAccessException extends StarRocksConnectorException {

    /**
     * "Nothing here to describe" - an unrepresentable shape, or no grant / no table - rather than "this attempt
     * failed". Enumeration leaves such a table out; everything else, AWS failures included, fails the statement.
     */
    private final boolean nothingToDescribe;

    public LakeFormationTableAccessException(String message) {
        this(message, null, false);
    }

    public LakeFormationTableAccessException(String message, Throwable cause) {
        this(message, cause, causeReportsNothingToDescribe(cause));
    }

    private LakeFormationTableAccessException(String message, Throwable cause, boolean nothingToDescribe) {
        super(message, cause);
        this.nothingToDescribe = nothingToDescribe;
    }

    public static LakeFormationTableAccessException nothingToDescribe(String message) {
        return new LakeFormationTableAccessException(message, null, true);
    }

    public static LakeFormationTableAccessException nothingToDescribe(String message, Throwable cause) {
        return new LakeFormationTableAccessException(message, cause, true);
    }

    public boolean isNothingToDescribe() {
        return nothingToDescribe;
    }

    /** Survives the rewrap each rethrow does. */
    private static boolean causeReportsNothingToDescribe(Throwable cause) {
        return cause instanceof LakeFormationTableAccessException lakeFormationCause
                && lakeFormationCause.isNothingToDescribe();
    }
}
