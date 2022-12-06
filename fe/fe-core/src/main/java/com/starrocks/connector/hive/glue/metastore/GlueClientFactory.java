// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.metastore;

import com.amazonaws.services.glue.AWSGlue;
import org.apache.hadoop.hive.metastore.api.MetaException;

/***
 * Interface for creating Glue AWS Client
 */
public interface GlueClientFactory {

    AWSGlue newClient() throws MetaException;

}
