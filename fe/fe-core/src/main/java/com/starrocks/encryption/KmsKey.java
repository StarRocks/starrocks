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
package com.starrocks.encryption;

import com.starrocks.common.Config;
import com.starrocks.proto.EncryptionAlgorithmPB;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.proto.EncryptionKeyTypePB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Base64;
import java.util.UUID;

import static com.starrocks.connector.share.iceberg.IcebergAwsClientFactory.tryToResolveRegion;

/**
 * Correspond to a key in KMS, store KMS key as arn(keyid)
 * only persist the key address in meta, not the actual plain key
 */
public class KmsKey extends EncryptionKey {
    private static final Logger LOG = LoggerFactory.getLogger(KmsKey.class);

    private String keyId;

    public static KmsKey createFromSpec(String spec) {
        return new KmsKey(spec);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KmsKey)) {
            return false;
        }
        return keyId.equalsIgnoreCase(((KmsKey) obj).keyId);
    }

    @Override
    public String toSpec() {
        return String.format("kms:%s", keyId);
    }

    public KmsKey() {
    }

    public KmsKey(String addr) {
        this.keyId = addr;
    }

    @Override
    public void toPB(EncryptionKeyPB pb, KeyMgr mgr) {
        super.toPB(pb, mgr);
        pb.type = EncryptionKeyTypePB.KMS_KEY;
        pb.keyDesc = keyId;
    }

    @Override
    public void fromPB(EncryptionKeyPB pb, KeyMgr mgr) {
        super.fromPB(pb, mgr);
        this.keyId = pb.keyDesc;
    }

    @Override
    public EncryptionKey generateKey() {
        LOG.info(String.format("generate KmsDataKey using KmsKey(%s)", keyId));
        KmsClient kmsClient = getKmsClient();
        GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
                .keyId(keyId)
                .keySpec(DataKeySpec.AES_128)
                .build();
        GenerateDataKeyResponse resp = kmsClient.generateDataKey(request);
        byte[] plaintext = resp.plaintext().asByteArray();
        String encrypted = Base64.getEncoder().encodeToString(resp.ciphertextBlob().asByteArray());
        KmsDataKey kmsDataKey = new KmsDataKey(EncryptionAlgorithmPB.AES_128, encrypted);
        kmsDataKey.setPlainKey(plaintext);
        kmsDataKey.setParent(this);
        return kmsDataKey;
    }

    @Override
    public void decryptKey(EncryptionKey key) {
        if (!(key instanceof KmsDataKey)) {
            throw new IllegalArgumentException("NormalKey cannot not decrypt " + key.getClass().getName());
        }
        KmsDataKey kmsDataKey = (KmsDataKey) key;
        kmsDataKey.setPlainKey(unwrapKmsDataKey(keyId, kmsDataKey.getEncryptedKey()));
    }

    @Override
    public String toString() {
        return String.format("KmsKey(id:%d createTime:%d addr:%s)", id, createTime, keyId);
    }

    public static byte[] unwrapKmsDataKey(String keyId, String encryptedDataKey) {
        LOG.info(String.format("decrypting KmsDataKey(%s) using KmsKey(%s)", encryptedDataKey, keyId));
        KmsClient kmsClient = getKmsClient();
        byte[] encryptedDecoded = Base64.getDecoder().decode(encryptedDataKey);
        return kmsClient.decrypt(builder -> builder.keyId(keyId).ciphertextBlob(
                        SdkBytes.fromByteArray(encryptedDecoded))).plaintext()
                .asByteArray();
    }

    public static KmsClient createKMSClient() {
        AwsCredentialsProvider provider;
        if (!Config.aws_kms_access_key.isEmpty() && !Config.aws_kms_secret_key.isEmpty()) {
            provider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(Config.aws_kms_access_key, Config.aws_kms_secret_key));
        } else {
            LOG.info("using DefaultCredentialsProvider for KMS");
            provider = DefaultCredentialsProvider.builder().build();
        }
        if (!Config.aws_kms_iam_role_arn.isEmpty()) {
            StsClientBuilder stsClientBuilder = StsClient.builder().credentialsProvider(provider);
            AssumeRoleRequest.Builder assumeRoleBuilder = AssumeRoleRequest.builder();
            assumeRoleBuilder.roleArn(Config.aws_kms_iam_role_arn);
            assumeRoleBuilder.roleSessionName(UUID.randomUUID().toString());
            if (!Config.aws_kms_external_id.isEmpty()) {
                assumeRoleBuilder.externalId(Config.aws_kms_external_id);
            }
            provider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClientBuilder.build())
                    .refreshRequest(assumeRoleBuilder.build())
                    .build();
        }
        KmsClientBuilder kmsClientBuilder = KmsClient.builder();
        kmsClientBuilder.credentialsProvider(provider);
        if (!Config.aws_kms_region.isEmpty()) {
            kmsClientBuilder.region(tryToResolveRegion(Config.aws_kms_region));
        }
        return kmsClientBuilder.build();
    }

    private static KmsClient kmsClient;

    public static synchronized KmsClient getKmsClient() {
        if (kmsClient == null) {
            kmsClient = createKMSClient();
        }
        return kmsClient;
    }

    public static void main(String[] args) {
        Config.aws_kms_region = "us-west-2";
        KmsKey kmsKey = KmsKey.createFromSpec("arn:aws:kms:us-west-2:081976408565:key/a1be82ad-f168-4ab1-b473-5d2f4ed77693");
        KmsDataKey key = (KmsDataKey) kmsKey.generateKey();
        System.out.println(key.getEncryptedKey());
        KmsDataKey key2 = new KmsDataKey(EncryptionAlgorithmPB.AES_128, key.getEncryptedKey());
        kmsKey.decryptKey(key2);
        // verify two plain keys' base64 are the same
        String p1 = Base64.getEncoder().encodeToString(key.getPlainKey());
        String p2 = Base64.getEncoder().encodeToString(key2.getPlainKey());
        System.out.println("p1: " + p1);
        System.out.println("p2: " + p2);
    }
}
