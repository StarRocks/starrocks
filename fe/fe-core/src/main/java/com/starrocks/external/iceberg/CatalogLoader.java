// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

import com.starrocks.external.iceberg.glue.IcebergGlueCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;

/**
 * Most code of this file was copied from CatalogLoader.java of flink in iceberg codebase.
 * Please see https://github.com/apache/iceberg.
 * // TODO: add hadoop catalogloader here.
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

    static CatalogLoader custom(String name, Configuration hadoopConf, Map<String, String> properties,
                                String catalogImpl) {
        return new CustomCatalogLoader(name, hadoopConf, properties, catalogImpl);
    }

    static CatalogLoader glue(String name, Configuration hadoopConf, Map<String, String> properties) {
        return new GlueCatalogLoader(name, hadoopConf, properties);
    }

    class GlueCatalogLoader implements CatalogLoader {
        private final String catalogName;
        private final SerializableConfiguration hadoopConf;
        private final int clientPoolSize;
        private final Map<String, String> properties;

        private GlueCatalogLoader(String catalogName, Configuration conf, Map<String, String> properties) {
            this.catalogName = catalogName;
            this.hadoopConf = new SerializableConfiguration(conf);
            this.clientPoolSize = properties.containsKey(CatalogProperties.CLIENT_POOL_SIZE) ?
                    Integer.parseInt(properties.get(CatalogProperties.CLIENT_POOL_SIZE)) :
                    CatalogProperties.CLIENT_POOL_SIZE_DEFAULT;
            this.properties = Maps.newHashMap(properties);
        }

        @Override
        public Catalog loadCatalog() {
            return CatalogUtil.loadCatalog(IcebergGlueCatalog.class.getName(), catalogName, properties,
                    hadoopConf.get());
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("catalogName", catalogName)
                    .add("clientPoolSize", clientPoolSize)
                    .toString();
        }
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
            return CatalogUtil.loadCatalog(IcebergHiveCatalog.class.getName(), catalogName, properties,
                    hadoopConf.get());
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

    class CustomCatalogLoader implements CatalogLoader {

        private final SerializableConfiguration hadoopConf;
        private final Map<String, String> properties;
        private final String name;
        private final String catalogImpl;

        private CustomCatalogLoader(
                String name,
                Configuration conf,
                Map<String, String> properties,
                String catalogImpl) {
            this.hadoopConf = new SerializableConfiguration(conf);
            this.properties = Maps.newHashMap(properties); // wrap into a hashmap for serialization
            this.name = name;
            this.catalogImpl = Preconditions.checkNotNull(catalogImpl,
                    "Cannot initialize custom GlobalStateMgr, impl class name is null");
        }

        @Override
        public Catalog loadCatalog() {
            return CatalogUtil.loadCatalog(catalogImpl, name, properties, hadoopConf.get());
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("catalogImpl", catalogImpl)
                    .toString();
        }
    }
}
