// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <aws/s3/S3Client.h>

#include "common/s3_uri.h"
#include "env/env.h"

namespace starrocks {

std::unique_ptr<Env> new_env_s3();

std::shared_ptr<Aws::S3::S3Client> new_s3client(const S3URI& uri);

} // namespace starrocks
