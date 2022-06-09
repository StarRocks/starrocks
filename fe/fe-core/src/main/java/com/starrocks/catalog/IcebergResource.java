// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.external.iceberg.IcebergCatalogType;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.starrocks.common.util.Util.validateMetastoreUris;

/**
 * Iceberg resource for external Iceberg table
 * <p>
 * Iceberg resource example:
 * CREATE EXTERNAL RESOURCE "iceberg0"
 * PROPERTIES
 * (
 * "type" = "iceberg",
 * "starrocks.catalog-type"="hive"
 * );
 * <p>
 * DROP RESOURCE "iceberg0";
 */
public class IcebergResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(IcebergResource.class);

    private static final String ICEBERG_CATALOG = "starrocks.catalog-type";
    private static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    private static final String ICEBERG_IMPL = "iceberg.catalog-impl";

    @SerializedName(value = "catalogType")
    private String catalogType;

    @SerializedName(value = "metastoreURIs")
    private String metastoreURIs;

    @SerializedName(value = "catalogImpl")
    private String catalogImpl;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public IcebergResource(String name) {
        super(name, ResourceType.ICEBERG);
        properties = Maps.newHashMap();
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkNotNull(properties, "Properties of iceberg resource is null!");

        catalogType = properties.get(ICEBERG_CATALOG);
        if (StringUtils.isBlank(catalogType)) {
            throw new DdlException(ICEBERG_CATALOG + " must be set in properties");
        }

        switch (IcebergCatalogType.fromString(catalogType)) {
            case HIVE_CATALOG:
                metastoreURIs = properties.get(ICEBERG_METASTORE_URIS);
                if (StringUtils.isBlank(metastoreURIs)) {
                    throw new DdlException(ICEBERG_METASTORE_URIS + " must be set in properties");
                }
                break;
            case CUSTOM_CATALOG:
                catalogImpl = properties.get(ICEBERG_IMPL);
                if (StringUtils.isBlank(catalogImpl)) {
                    throw new DdlException(ICEBERG_IMPL + " must be set in properties");
                }
                try {
                    Thread.currentThread().getContextClassLoader().loadClass(catalogImpl);
                } catch (ClassNotFoundException e) {
                    throw new DdlException("Unknown class: " + catalogImpl);
                }
                break;
            default:
                throw new DdlException("Unexpected globalStateMgr type: " + catalogType);
        }
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        switch (IcebergCatalogType.fromString(catalogType)) {
            case HIVE_CATALOG:
                result.addRow(Lists.newArrayList(name, lowerCaseType, ICEBERG_METASTORE_URIS, metastoreURIs));
                break;
            case CUSTOM_CATALOG:
                result.addRow(Lists.newArrayList(name, lowerCaseType, ICEBERG_IMPL, catalogImpl));
                break;
            default:
                LOG.warn("Unexpected globalStateMgr type: " + catalogType);
                break;
        }
    }

    public String getHiveMetastoreURIs() {
        return metastoreURIs;
    }

    public String getIcebergImpl() {
        return catalogImpl;
    }

    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.fromString(catalogType);
    }

    /**
     * <p>alter the resource properties.</p>
     * <p>the user can not alter the property that the system does not support.
     *
     * @param properties the properties that user uses to alter
     * @throws DdlException
     */
    public void alterProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null, "properties can not be null");

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (ICEBERG_METASTORE_URIS.equals(key)) {
                if (StringUtils.isBlank(value)) {
                    throw new DdlException(ICEBERG_METASTORE_URIS + " can not be null");
                }
                validateMetastoreUris(value);
                this.metastoreURIs = value;
            } else if (ICEBERG_IMPL.equals(key)) {
                if (StringUtils.isBlank(value)) {
                    throw new DdlException(ICEBERG_IMPL + " can not be null");
                }
                this.catalogImpl = value;
            } else {
                throw new DdlException(String.format("property %s has not support yet", key));
            }
        }
    }
}
