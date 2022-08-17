// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.BaseProcResult;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

import static com.starrocks.common.util.Util.validateMetastoreUris;

/**
 * Hive resource for external hive table
 * <p>
 * Hive resource example:
 * CREATE EXTERNAL RESOURCE "hive0"
 * PROPERTIES
 * (
 * "type" = "hive",
 * "hive.metastore.uris" = "thrift://hostname:9083"
 * );
 * <p>
 * DROP RESOURCE "hive0";
 */
public class HiveResource extends Resource {

    // require only one property currently
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    @SerializedName(value = "metastoreURIs")
    private String metastoreURIs;

    public HiveResource(String name) {
        super(name, ResourceType.HIVE);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null, "properties can not be null");

        metastoreURIs = properties.get(HIVE_METASTORE_URIS);
        if (!FeConstants.runningUnitTest) {
            if (StringUtils.isBlank(metastoreURIs)) {
                throw new DdlException(HIVE_METASTORE_URIS + " must be set in properties");
            }
            validateMetastoreUris(metastoreURIs);
        }
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, HIVE_METASTORE_URIS, metastoreURIs));
    }

    public String getHiveMetastoreURIs() {
        return metastoreURIs;
    }

    /**
     * <p>alter the resource properties.</p>
     * <p>the user can not alter the property that the system does not support.
     * currently , hive resource only support 'hive.metastore.uris' property to alter. </p>
     *
     * @param properties the properties that user uses to alter
     * @throws DdlException
     */
    public void alterProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null, "properties can not be null");

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (HIVE_METASTORE_URIS.equals(key)) {
                if (StringUtils.isBlank(value)) {
                    throw new DdlException(HIVE_METASTORE_URIS + " can not be null");
                }
                validateMetastoreUris(value);
                this.metastoreURIs = value;
            } else {
                throw new DdlException(String.format("property %s has not support yet", key));
            }
        }
    }
}
