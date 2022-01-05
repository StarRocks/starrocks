// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;

/**
 * Most code of this file was copied from CatalogLoader.java of flink in iceberg codebase.
 * Please see https://github.com/apache/iceberg.
 * // TODO: add hadoop or custom catalogloader here.
 */
public interface CatalogLoader {

    /**
     * Create a new catalog with the provided properties.
     *
     * @return a newly created {@link Catalog}
     */
    Catalog loadCatalog();

    static CatalogLoader hive(String name, Configuration hadoopConf, Map<String, String> properties) {
        return new HiveCatalogLoader(name, hadoopConf, properties);
    }

    class HiveCatalogLoader implements CatalogLoader {
        private final String catalogName;
        private final SerializableConfiguration hadoopConf;
        private final String uri;
        private final int clientPoolSize;
        private final Map<String, String> properties;

        private HiveCatalogLoader(String catalogName, Configuration conf, Map<String, String> properties) {
            this.catalogName = catalogName;
            this.hadoopConf = new SerializableConfiguration(conf);
            this.uri = properties.get(CatalogProperties.URI);
            this.clientPoolSize = properties.containsKey(CatalogProperties.CLIENT_POOL_SIZE) ?
                Integer.parseInt(properties.get(CatalogProperties.CLIENT_POOL_SIZE)) :
                CatalogProperties.CLIENT_POOL_SIZE_DEFAULT;
            this.properties = Maps.newHashMap(properties);
        }

        @Override
        public Catalog loadCatalog() {
            return CatalogUtil.loadCatalog(HiveCatalog.class.getName(), catalogName, properties, hadoopConf.get());
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("catalogName", catalogName)
                .add("uri", uri)
                .add("clientPoolSize", clientPoolSize)
                .toString();
        }
    }

}
