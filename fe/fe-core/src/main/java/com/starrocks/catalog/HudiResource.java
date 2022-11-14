// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

import static com.starrocks.common.util.Util.validateMetastoreUris;

/**
 * Hive resource for external hudi table
 * <p>
 * Hudi resource example:
 * CREATE EXTERNAL RESOURCE "hudi0"
 * PROPERTIES
 * (
 * "type" = "hudi",
 * "hive.metastore.uris" = "thrift://hostname:9083"
 * );
 * <p>
 * DROP RESOURCE "hudi0";
 */
public class HudiResource extends Resource {
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    @SerializedName(value = "metastoreURIs")
    private String metastoreURIs;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HudiResource(String name) {
        super(name, ResourceType.HUDI);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.properties = Maps.newHashMap(properties);
        metastoreURIs = properties.get(HIVE_METASTORE_URIS);
        if (StringUtils.isBlank(metastoreURIs)) {
            throw new DdlException(HIVE_METASTORE_URIS + " must be set in properties");
        }
        validateMetastoreUris(metastoreURIs);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, HIVE_METASTORE_URIS, metastoreURIs));
    }

    public String getHiveMetastoreURIs() {
        return metastoreURIs;
    }

    public Map<String, String> getProperties() {
        return properties == null ? Maps.newHashMap() : properties;
    }

    /**
     * <p>alter the resource properties.</p>
     * <p>the user can not alter the property that the system does not support.
     * currently , hudi resource only support 'hive.metastore.uris' property to alter. </p>
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
