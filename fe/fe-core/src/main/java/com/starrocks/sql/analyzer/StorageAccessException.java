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

import org.apache.commons.lang.exception.ExceptionUtils;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
        if (rootCause instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) rootCause;
            AwsErrorDetails awsErrorDetails = s3Exception.awsErrorDetails();
            if (awsErrorDetails.errorCode() != null) {
                builder.append("Error code: ").append(awsErrorDetails.errorCode()).append(". ");
            }
            if (awsErrorDetails.errorMessage() != null) {
                builder.append("Error message: ").append(awsErrorDetails.errorMessage());
            } else {
                builder.append("Error message: ").append(s3Exception.getMessage());
            }
        } else {
            builder.append("Error message: ").append(rootCause.getMessage());
        }
        // TODO: translate error message of other storage systems
        return builder.toString();
    }
}
