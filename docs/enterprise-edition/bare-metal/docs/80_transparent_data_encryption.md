# Transparent Data Encryption

This topic describes how to enable Transparent Data Encryption (TDE) to protect your data in your StarRocks shared-data clusters.

The StarRocks Enterprise Edition supports Transparent Data Encryption from v3.3.4.

## Overview

Transparent Data Encryption (TDE) is a technology designed to protect database data. It encrypts and decrypts stored data at the database engine layer, ensuring that the process is transparent to users and applications. TDE encrypts data when it is written to the disk, ensuring that all stored data is secure, and automatically decrypts data when accessed by applications, eliminating the need for any modifications to the application.

The primary advantages of TDE include:

- **Compliance Requirements**: TDE helps companies meet regulatory requirements, such as GDPR and HIPAA, by ensuring that stored sensitive data is encrypted.
- **Reduced Risk of Data Breach**: Even if database files are illegally accessed or stolen, unauthorized users cannot read the data.
- **Ease of Use**: TDE is transparent to applications and there is no need to modify them, providing efficient encryption protection.

When TDE is enabled, all user data is encrypted on disk. Users can manage their encryption keys independently or use third-party key management tools such AWS KMS or HashiCorp Vault.

The encryption and decryption process is as follows:

1. Users specify the type and secret of the Master Key when creating a cluster.
2. StarRocks uses the Master Key to generate and periodically rotate the Key Encryption Key (KEK).
3. For each new data file, a new Data Encryption Key (DEK) is generated using the KEK. A DEK consists of the plaintext part and the ciphertext part. The data files are encrypted using the plaintext part, while the ciphertext part is stored in the metadata.
4. When users access data, StarRocks retrieves the DEK ciphertext part from the metadata, and decrypts it using the KEK to obtain the DEK plaintext part, which is then used to decrypt the data file.

## Usage

To enable Transparent Data Encryption for StarRocks, **you must configure the cryptographic key service while deploying the cluster**.

### Enable TDE with Vault

When Vault is used for key management, the DEK plaintext part will be stored in Vault. Each time the cryptographic key is required, StarRocks requests it from Vault and caches it in memory to avoid storing it in local disks.

To enable Transparent Data Encryption with Vault, you must provide your key property (secret path) and authentication information to allow StarRocks access to the secret path you set up in Vault. The authentication properties include the Vault server address and the service token, which can be set either in StarRocks configuration files or through environment variables. If you have set them in both ways, configuration files will take precedence.

You must create the secret path using the following command on your Vault server:

```Bash
# The plain_key must be in the format `aes_128:<Base64 encoding of the 128 bit key>`.
# Example: plain_key=aes_128:3bozYSHPqtPi49TMQU1T4g==
vault kv put -mount=secret starrocks plain_key=aes_128:<Base64 encoding of the 128 bit key>
```

For more instructions on how to obtain a token, see the [Vault official documentation](https://developer.hashicorp.com/vault/docs/concepts/tokens).

In the FE configuration file **fe.conf**, you must set the following items:

```Properties
# The address of your Vault server, for example, http://127.0.0.1:8200.
vault_addr=<vault_server_address>

# The Vault service token, for example, hvs.PCDYqtwjwR3jZ1YUfqKvK9ns.
vault_token=<vault_service_token>

# The secret path you set up for StarRocks in Vault.
# It must be prefixed with `vault:`, for example, vault:/v1/secret/data/starrocks.
default_master_key=<secret_path>
```

In the CN configuration file **cn.conf**, you must set the following items:

```Properties
# The address of your Vault server, for example, http://127.0.0.1:8200.
vault_addr=<vault_server_address>

# The Vault service token, for example, hvs.PCDYqtwjwR3jZ1YUfqKvK9ns.
vault_token=<vault_service_token>

# Whether to enable TDE. Set this value to true.
enable_transparent_data_encryption=true
```

`vault_addr` and `vault_token` can be set through environment variables for each FE and CN instance.

```Bash
export VAULT_ADDR=<vault_server_address>
export VAULT_TOKEN=<vault_service_token>
```

### Enable TDE with AWS KMS

KMS will manage the Master Key and generate KEK with its GenerateDataKey API. StarRocks will not store or cache the cryptographic key.

To enable Transparent Data Encryption with AWS KMS, you must provide your key ID and the AWS region of your KMS service. And if you use credentials other than instance profiles to access AWS, you will also need to provide credential information to allow StarRocks access to the cryptographic key stored in KMS. The credential information can be set either in StarRocks configuration files or through environment variables. If you have set them in both ways, configuration files will take precedence.

In the FE configuration file **fe.conf**, you must set the following items:

```Properties
# The key ID in AWS KMS. It must be prefixed with `kms:`.
# Key ID example: kms:a1b2c3d4e5-f6g7-h8i9-j0k1-l2m3n4o5p6q7
#                 kms:mrk-12ab3c45d6789ef0gh12ijkl345678mn
default_master_key=<key_id>

aws_kms_region=<aws_kms_region>
```

For more instructions on how to obtain the key ID, see the [AWS KMS official documentation](https://docs.aws.amazon.com/kms/latest/developerguide/find-cmk-id-arn.html).

In the CN configuration file **cn.conf**, you must set the following item:

```Properties
# Whether to enable TDE. Set this value to true.
enable_transparent_data_encryption=true
```

If you use credentials other than Instance Profile to access AWS, you will need to add these items to both FE and CN configuration files:

```Properties
# If you use IAM user-based credentials, add these items:
aws_kms_access_key=<aws_kms_access_key_id>
aws_kms_secret_key=<aws_kms_secret_access_key>

# If you use Assumed Role, add this item:
aws_kms_iam_role_arn=<aws_kms_iam_role_arn>

# If you use Assumed Role from an external account, add these items:
aws_kms_iam_role_arn=<aws_kms_iam_role_arn>
aws_kms_external_id=<aws_kms_external_id>
```

`aws_kms_region`, `aws_kms_access_key`, and `aws_kms_secret_key` can be set through environment variables for each FE and CN instance.

```Bash
export AWS_REGION=<aws_kms_region>
export AWS_ACCESS_KEY_ID=<aws_kms_access_key_id>
export AWS_SECRET_ACCESS_KEY=<aws_kms_secret_access_key>
```

## Observability

StarRocks provides a variety of metrics for monitoring the Transparent Data Encryption feature.

#### encryption_keys_created

- Unit: -
- Type: Cumulative
- Description: Number of file encryption keys created for file encryption.

#### encryption_keys_unwrapped

- Unit: -
- Type: Cumulative
- Description: This metric records the total number of decryption operations.

#### encryption_keys_in_cache

- Unit: -
- Type: Instantaneous
- Description: Number of encryption keys currently in the key cache.

#### encryption_bytes

- Unit: Byte
- Type: Cumulative
- Description: Total number of bytes encrypted.

#### decryption_bytes

- Unit: Byte
- Type: Cumulative
- Description: Total number of bytes decrypted.

## Limitations

- Currently, Transparent Data Encryption is only supported in shared-data clusters.
- Enabling Transparent Data Encryption for an existing cluster or modifying the Master Key configurations is not supported. The Master Key cannot be changed after setup.
- Enabling Transparent Data Encryption can cause a performance loss of less than 10%. 