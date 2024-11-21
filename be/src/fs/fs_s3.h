// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>

#include "fs/credential/cloud_configuration.h"
#include "fs/fs.h"
#include "util/random.h"

namespace Aws::S3 {
class S3Client;
}

namespace starrocks {

std::unique_ptr<FileSystem> new_fs_s3(const FSOptions& options);
void close_s3_clients();

class S3ClientFactory {
public:
    using ClientConfiguration = Aws::Client::ClientConfiguration;
    using S3Client = Aws::S3::S3Client;
    using S3ClientPtr = std::shared_ptr<S3Client>;
    using ClientConfigurationPtr = std::shared_ptr<ClientConfiguration>;
    using AWSCloudConfigurationPtr = std::shared_ptr<AWSCloudConfiguration>;

    static S3ClientFactory& instance() {
        static S3ClientFactory obj;
        return obj;
    }

    // Indicates the different S3 operation of using the client.
    // This class is used to set different configuration for clients
    // with different purposes.
    enum class OperationType {
        UNKNOWN,
        RENAME_FILE,
    };

    ~S3ClientFactory() = default;

    S3ClientFactory(const S3ClientFactory&) = delete;
    void operator=(const S3ClientFactory&) = delete;
    S3ClientFactory(S3ClientFactory&&) = delete;
    void operator=(S3ClientFactory&&) = delete;

    S3ClientPtr new_client(const TCloudConfiguration& cloud_configuration,
                           S3ClientFactory::OperationType operation_type = S3ClientFactory::OperationType::UNKNOWN);
    S3ClientPtr new_client(const ClientConfiguration& config, const FSOptions& opts);

    void close();

    static ClientConfiguration& getClientConfig() {
        // We cached config here and make a deep copy each time.Since aws sdk has changed the
        // Aws::Client::ClientConfiguration default constructor to search for the region
        // (where as before 1.8 it has been hard coded default of "us-east-1").
        // Part of that change is looking through the ec2 metadata, which can take a long time.
        // For more details, please refer https://github.com/aws/aws-sdk-cpp/issues/1440
        static ClientConfiguration instance;
        return instance;
    }

    // Only use for UT
    bool find_client_cache_keys_by_config_TEST(const Aws::Client::ClientConfiguration& config,
                                               AWSCloudConfiguration* cloud_config = nullptr) {
        return _find_client_cache_keys_by_config_TEST(config);
    }

private:
    S3ClientFactory();

    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider> _get_aws_credentials_provider(
            const AWSCloudCredential& aws_cloud_credential);

    class ClientCacheKey {
    public:
        ClientConfigurationPtr config;
        AWSCloudConfigurationPtr aws_cloud_configuration;

        bool operator==(const ClientCacheKey& rhs) const;
    };

    constexpr static int kMaxItems = 8;

    // Only use for UT
    bool _find_client_cache_keys_by_config_TEST(const Aws::Client::ClientConfiguration& config,
                                                AWSCloudConfiguration* cloud_config = nullptr);

    std::mutex _lock;
    int _items{0};
    // _client_cache_keys[i] is the client cache key of |_clients[i].
    ClientCacheKey _client_cache_keys[kMaxItems];
    S3ClientPtr _clients[kMaxItems];
    Random _rand;
};

} // namespace starrocks
