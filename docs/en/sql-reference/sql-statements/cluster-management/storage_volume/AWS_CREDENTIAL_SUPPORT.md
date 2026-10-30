---
displayed_sidebar: docs
description: "Compare AWS credential support in the FE runtime and StarOS-backed storage volumes."
sidebar_position: 10
---

# AWS credential support for storage volumes

StarRocks FE uses `AwsCloudCredential` to configure AWS authentication for storage volumes and other AWS consumers.
Storage volumes additionally serialize their credentials to StarOS. Some combinations supported by the FE runtime cannot
yet be preserved in a storage volume.

## Support matrix

| Authentication method | Required properties | FE runtime | Storage volume | Notes |
|---|---|:---:|:---:|---|
| AWS SDK default chain | `aws.s3.use_aws_sdk_default_behavior=true` | Supported | Supported | Uses the AWS SDK default credential provider chain. |
| Default chain followed by AssumeRole | `aws.s3.use_aws_sdk_default_behavior=true`<br />`aws.s3.iam_role_arn=<arn>`<br />Optional: `aws.s3.external_id` | Supported | Not supported | The storage volume preserves only the default-chain selection. |
| Instance Profile | `aws.s3.use_instance_profile=true` | Supported | Supported | Uses EC2 Instance Profile credentials. |
| Instance Profile followed by AssumeRole | `aws.s3.use_instance_profile=true`<br />`aws.s3.iam_role_arn=<arn>`<br />Optional: `aws.s3.external_id` | Supported | Supported | The role ARN and external ID are preserved. |
| Web Identity Token file | `aws.s3.use_web_identity_token_file=true`<br />Worker environment: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN` | Supported | Supported | The token file and primary role are resolved on each worker. |
| Web Identity followed by a second AssumeRole operation | `aws.s3.use_web_identity_token_file=true`<br />`aws.s3.iam_role_arn=<second_hop_arn>`<br />Optional: `aws.s3.external_id`<br />Worker environment: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN` | Supported | Supported | The configured role ARN represents the second STS hop. |
| Static Access Key and Secret Key | `aws.s3.access_key=<access_key>`<br />`aws.s3.secret_key=<secret_key>` | Supported | Supported | The Access Key and Secret Key are preserved. |
| Static session credentials | Access Key and Secret Key properties<br />`aws.s3.session_token=<token>` | Supported | Not supported | The session token is not preserved by the storage volume. |
| Access Key and Secret Key followed by AssumeRole | Access Key and Secret Key properties<br />`aws.s3.iam_role_arn=<arn>`<br />Optional: `aws.s3.external_id` | Supported | Not supported | The AssumeRole settings are not preserved by the storage volume. |
| Custom STS region or endpoint | Valid base credentials<br />`aws.s3.sts.region=<region>` and/or `aws.s3.sts.endpoint=<endpoint>`<br />`aws.s3.iam_role_arn=<arn>` | Supported | Not supported | Custom STS settings are not preserved by the storage volume. |
| Role ARN without base credentials | `aws.s3.iam_role_arn=<arn>` only | Not supported | Not supported | AssumeRole requires a valid base credential provider. |

Do not configure a storage volume with a combination marked **Not supported**. Although FE can construct the credential
provider, the complete configuration is not retained when the storage volume is serialized to StarOS.

## Credential resolution order

If you configure multiple base authentication methods, FE selects the first matching method in this order:

```text
AWS SDK default chain > Instance Profile > Web Identity > Access Key and Secret Key
```

After selecting the base credential provider, FE performs an STS AssumeRole operation when `aws.s3.iam_role_arn` is not
empty.

## Switching authentication methods

`ALTER STORAGE VOLUME` merges the properties you specify into the storage volume's existing properties instead of
replacing them. Combined with the resolution order above, a statement that sets only the new method's properties can
succeed without changing the credential that the storage volume actually uses.

To switch a storage volume from one authentication method to another:

- Set every `use_*` property explicitly, with exactly one of them set to `true`.
- Clear `aws.s3.iam_role_arn` and `aws.s3.external_id` unless the new method must also assume a role. These are not
  `use_*` properties, so they survive the merge and are applied to the new method.

### Statements that do not switch the method

Each statement below succeeds, but the storage volume keeps its previous credential, and the properties you supplied
are not stored.

```SQL
-- The storage volume currently uses a web identity token file.
-- No effect: Web Identity outranks Access Key and Secret Key.
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- The storage volume currently uses Instance Profile.
-- No effect: Instance Profile outranks Web Identity.
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_web_identity_token_file" = "true"
);
```

### Statements that switch the method

```SQL
-- Web Identity to Access Key and Secret Key.
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.use_web_identity_token_file" = "false",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- Instance Profile to Web Identity, dropping the previous Role.
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.use_web_identity_token_file" = "true",
    "aws.s3.iam_role_arn" = "",
    "aws.s3.external_id" = ""
);
```

Switching to a method that outranks the current one also works without disabling the current method, because the new
method is matched first. For example, a storage volume that uses Access Key and Secret Key switches to Web Identity
when you set only `aws.s3.use_web_identity_token_file` to `true`. Setting every `use_*` property explicitly works in
both directions and does not require you to know which method is currently in effect.

:::caution
A Role ARN left over from the previous method is applied to the new method. If a storage volume used Web Identity with
a second AssumeRole hop and you switch it to Instance Profile, the leftover `aws.s3.iam_role_arn` becomes the role that
Instance Profile assumes. Clear `aws.s3.iam_role_arn` and `aws.s3.external_id` in the same statement to avoid this.
:::

For storage-volume configuration examples, see [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md#credential-information).
