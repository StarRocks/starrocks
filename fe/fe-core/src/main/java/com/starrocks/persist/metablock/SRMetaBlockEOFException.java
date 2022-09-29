// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist.metablock;

/**
 * This exception indicates a permitted EOF and should be captured and ignored.
 * A common scenario is when FE rollbacks after upgraded to a higher version and generated a image with more metadata
 */
public class SRMetaBlockEOFException extends Exception {

    public SRMetaBlockEOFException(String msg) {
        super(msg);
    }
}
