// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.analyzer;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.starrocks.common.exception.DdlException;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * Access remote storage(s3/gcs/oss/hdfs) exception
 */
public class StorageAccessException extends RuntimeException {

    public StorageAccessException(Throwable e) {
        super(e);
    }

    public Throwable getRootCause() {
        return ExceptionUtils.getRootCause(this);
    }

    @Override
    public String getMessage() {
        StringBuilder builder = new StringBuilder("Access storage error. ");
        Throwable rootCause = getRootCause();
        if (rootCause instanceof AmazonS3Exception) {
            AmazonS3Exception s3Exception = (AmazonS3Exception) rootCause;
            builder.append("Error code: ").append(s3Exception.getErrorCode()).append(". ");
            builder.append("Error message: ").append(s3Exception.getErrorMessage()).append(". ");
        } else if (rootCause instanceof DdlException) {
            builder.append("Error message: ").append(rootCause.getMessage());
        } else {
            builder.append("Unknown error");
        }
        // TODO: translate error message of other storage systems
        return builder.toString();
    }
}
