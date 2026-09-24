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

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Turns AWS failures into messages an operator can act on.
 *
 * Raw SDK exceptions name neither the table nor what to do about it, and the two that matter most
 * here - a permission-type mismatch and a rejected credential duration - both read as generic
 * invalid-input errors. Diagnosing either from the raw text costs a support round trip, so every
 * mapped message carries the object, the reason, and where the limit comes from.
 */
public final class LakeFormationErrors {

    private LakeFormationErrors() {
    }

    public static LakeFormationTableAccessException metadataFailure(LakeFormationTableIdentity identity,
                                                                    Exception cause) {
        // Matched by type, not by simple name: "AccessDeniedException" is the simple name of a Glue
        // class, a Lake Formation class AND com.starrocks.authorization.AccessDeniedException, so a
        // name comparison would dress an internal StarRocks failure up as a missing LF grant.
        String detail;
        // No grant and no table mean "nothing to describe"; a permission type mismatch or an unclassified failure must fail.
        boolean nothingToDescribe = false;
        if (cause instanceof software.amazon.awssdk.services.glue.model.PermissionTypeMismatchException) {
            detail = "Lake Formation refused the permission types StarRocks declared. Phase 1 supports "
                    + "column-level permissions only; row filters and cell filters are not supported yet";
        } else if (cause instanceof software.amazon.awssdk.services.glue.model.AccessDeniedException) {
            detail = "the query's principal has no Lake Formation grant on this table, or the "
                    + "StarRocks role is not registered as an authorized caller";
            nothingToDescribe = true;
        } else if (cause instanceof software.amazon.awssdk.services.glue.model.EntityNotFoundException) {
            detail = "the table does not exist in this Glue catalog, or it is not registered with "
                    + "Lake Formation in this account and region";
            nothingToDescribe = true;
        } else {
            detail = "the Lake Formation metadata request failed";
        }
        String message =
                "Cannot read Lake Formation metadata for " + identity + ": " + detail + "." + diagnostics(cause);
        return nothingToDescribe
                ? LakeFormationTableAccessException.nothingToDescribe(message, cause)
                : new LakeFormationTableAccessException(message, cause);
    }

    public static LakeFormationTableAccessException vendingFailure(LakeFormationTableIdentity identity,
                                                                   int requestedDurationSeconds,
                                                                   Exception cause) {
        String detail;
        if (cause instanceof software.amazon.awssdk.services.lakeformation.model.InvalidInputException) {
            // The duration is the input most likely to be refused, and the message never says so.
            detail = "the request was rejected. The most common cause is that "
                    + LakeFormationCatalogProperties.CREDENTIAL_DURATION_SECONDS + "="
                    + requestedDurationSeconds + " exceeds what this account allows for the role; lower it "
                    + "with ALTER CATALOG. Note that StarRocks never retries with a shorter duration, "
                    + "because a silent downgrade would hide the real limit. The ceiling is often lower "
                    + "than the 43200s the API accepts: the session this catalog uses is a tagged one, "
                    + "which is role chaining, and AWS caps that at one hour - 3600 is the value observed "
                    + "to work where 21600 and 43200 were both refused";
        } else if (cause instanceof software.amazon.awssdk.services.lakeformation.model.AccessDeniedException) {
            detail = "Lake Formation refused to vend credentials for this table. The data location must be "
                    + "registered with Lake Formation and the principal must hold a grant on it";
        } else {
            detail = "the credential request failed";
        }
        return new LakeFormationTableAccessException(
                "Cannot obtain Lake Formation credentials for " + identity + ": " + detail + "." + diagnostics(cause),
                cause);
    }

    /**
     * Region, AWS error code and request id: without them an operator cannot open a support case or
     * tell two identically-worded refusals apart.
     */
    private static String diagnostics(Exception cause) {
        if (!(cause instanceof AwsServiceException)) {
            return " Cause: " + cause.getMessage();
        }
        AwsServiceException awsException = (AwsServiceException) cause;
        StringBuilder text = new StringBuilder();
        if (awsException.awsErrorDetails() != null) {
            text.append(" AWS error code: ").append(awsException.awsErrorDetails().errorCode()).append('.');
            if (awsException.awsErrorDetails().errorMessage() != null) {
                text.append(" AWS message: ").append(awsException.awsErrorDetails().errorMessage()).append('.');
            }
        }
        if (awsException.requestId() != null) {
            text.append(" Request ID: ").append(awsException.requestId()).append('.');
        }
        return text.toString();
    }
}
