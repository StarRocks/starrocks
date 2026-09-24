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

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

/**
 * Builds the TableArn GetTemporaryGlueTableCredentials requires. The account comes from the Glue response, then
 * the configured catalog id; never from STS, which names the caller rather than the owner.
 */
final class LakeFormationTableArns {

    private LakeFormationTableArns() {
    }

    static String of(LakeFormationTableIdentity identity,
                     software.amazon.awssdk.services.glue.model.Table glueTable,
                     LakeFormationCatalogProperties properties) {
        String account = glueTable == null ? null : glueTable.catalogId();
        if (StringUtils.isBlank(account)) {
            account = properties.awsCatalogId();
        }
        if (StringUtils.isBlank(account)) {
            throw new LakeFormationTableAccessException("Cannot build the Glue table ARN for " + identity
                    + ": Lake Formation returned no catalog id for it and none is configured. Set"
                    + " " + LakeFormationCatalogProperties.GLUE_CATALOG_ID + " on the catalog.");
        }
        return "arn:" + partitionFor(identity.region()) + ":glue:" + identity.region() + ":" + account
                + ":table/" + identity.dbName() + "/" + identity.tableName();
    }

    /** China and GovCloud have their own partitions; a wrong one is rejected as malformed. */
    private static String partitionFor(String region) {
        String normalized = region == null ? "" : region.toLowerCase(Locale.ROOT);
        if (normalized.startsWith("cn-")) {
            return "aws-cn";
        }
        if (normalized.startsWith("us-gov-")) {
            return "aws-us-gov";
        }
        return "aws";
    }
}
