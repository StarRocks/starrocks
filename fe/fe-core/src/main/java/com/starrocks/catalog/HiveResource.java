// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

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
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    @SerializedName(value = "metastoreURIs")
    private String metastoreURIs;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HiveResource(String name) {
        super(name, ResourceType.HIVE);
        properties = Maps.newHashMap();
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        metastoreURIs = properties.get(HIVE_METASTORE_URIS);
        if (StringUtils.isBlank(metastoreURIs)) {
            throw new DdlException(HIVE_METASTORE_URIS + " must be set in properties");
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
}
