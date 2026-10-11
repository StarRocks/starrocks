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

use super::Result;
use std::collections::HashMap;
type Properties = HashMap<String, String>;

fn copy(source: &Properties, key: &str, target: &mut Properties, option: &str) {
    if let Some(value) = source.get(key) {
        target.insert(option.into(), value.clone());
    }
}
fn flag(source: &Properties, key: &str) -> bool {
    source
        .get(key)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

// Private mapping of StarRocks TCloudConfiguration to Lance object_store options.
// Numeric values match CloudConfiguration.thrift; unsupported providers fail closed.
pub(super) fn options(uri: &str, cloud_type: i32, cloud: &Properties) -> Result<Properties> {
    let mut out = Properties::new();
    match cloud_type {
        0 => (),
        1 => aws(cloud, &mut out)?,
        2 => azure(uri, cloud, &mut out)?,
        _ => return Err("Unsupported Lance catalog cloud configuration"),
    }
    Ok(out)
}
fn aws(cloud: &Properties, out: &mut Properties) -> Result<()> {
    if [
        "aws.s3.iam_role_arn",
        "aws.s3.external_id",
        "aws.s3.sts.endpoint",
        "aws.s3.sts.region",
    ]
    .iter()
    .any(|k| cloud.contains_key(*k))
    {
        return Err("Lance does not support catalog-configured AWS STS role assumption");
    }
    if flag(cloud, "aws.s3.use_instance_profile")
        || flag(cloud, "aws.s3.use_web_identity_token_file")
    {
        return Err("Use the native default credential chain for Lance BE identity credentials");
    }
    if !flag(cloud, "aws.s3.use_aws_sdk_default_behavior") {
        if cloud.contains_key("aws.s3.access_key") != cloud.contains_key("aws.s3.secret_key") {
            return Err("Lance S3 access key and secret key must be supplied together");
        }
        copy(cloud, "aws.s3.access_key", out, "aws_access_key_id");
        copy(cloud, "aws.s3.secret_key", out, "aws_secret_access_key");
        copy(cloud, "aws.s3.session_token", out, "aws_session_token");
    }
    copy(cloud, "aws.s3.region", out, "aws_region");
    if let Some(endpoint) = cloud.get("aws.s3.endpoint") {
        let endpoint = if endpoint.contains("://") {
            endpoint.clone()
        } else {
            let scheme = if cloud
                .get("aws.s3.enable_ssl")
                .is_some_and(|v| v.eq_ignore_ascii_case("false"))
            {
                "http"
            } else {
                "https"
            };
            format!("{scheme}://{endpoint}")
        };
        if endpoint.starts_with("http://") {
            out.insert("aws_allow_http".into(), "true".into());
        }
        out.insert("aws_endpoint".into(), endpoint);
    }
    if cloud.contains_key("aws.s3.enable_path_style_access") {
        out.insert(
            "aws_virtual_hosted_style_request".into(),
            (!flag(cloud, "aws.s3.enable_path_style_access")).to_string(),
        );
    }
    Ok(())
}
fn scoped<'a>(cloud: &'a Properties, key: &str, host: &str) -> Option<&'a String> {
    cloud
        .get(&format!("{key}.{host}"))
        .or_else(|| cloud.get(key))
}
fn azure(uri: &str, cloud: &Properties, out: &mut Properties) -> Result<()> {
    let uri = url::Url::parse(uri).map_err(|_| "Invalid Azure dataset URI")?;
    let host = uri.host_str().ok_or("Missing Azure account host")?;
    let container = uri.username();
    if !matches!(uri.scheme(), "abfs" | "abfss" | "wasb" | "wasbs")
        || container.is_empty()
        || !(host.ends_with(".dfs.core.windows.net") || host.ends_with(".blob.core.windows.net"))
    {
        return Err("Lance Azure credentials require an account-qualified abfss or wasbs URI");
    }
    let account = host.split('.').next().ok_or("Missing Azure account")?;
    let blob_host = format!("{account}.blob.core.windows.net");
    out.insert("azure_storage_account_name".into(), account.into());
    let key = scoped(cloud, "fs.azure.account.key", host)
        .or_else(|| scoped(cloud, "fs.azure.account.key", &blob_host));
    let sas = scoped(cloud, "fs.azure.sas.fixed.token", host)
        .or_else(|| cloud.get(&format!("fs.azure.sas.{container}.{blob_host}")));
    if let Some(key) = key {
        out.insert("azure_storage_account_key".into(), key.clone());
    } else if let Some(sas) = sas {
        out.insert(
            "azure_storage_sas_key".into(),
            sas.trim_start_matches('?').into(),
        );
    } else {
        let provider = scoped(cloud, "fs.azure.account.oauth.provider.type", host)
            .ok_or("Unsupported or mismatched Lance Azure credentials")?;
        if let Some(id) = scoped(cloud, "fs.azure.account.oauth2.client.id", host) {
            out.insert("azure_storage_client_id".into(), id.clone());
        }
        if provider.ends_with(".MsiTokenProvider") {
            return Ok(());
        }
        if !provider.ends_with(".ClientCredsTokenProvider") {
            return Err("Unsupported Azure OAuth provider");
        }
        if !out.contains_key("azure_storage_client_id") {
            return Err("Missing Azure OAuth client ID");
        }
        let authority = scoped(cloud, "fs.azure.account.oauth2.client.endpoint", host)
            .ok_or("Missing Azure OAuth authority")?;
        let authority = url::Url::parse(authority).map_err(|_| "Invalid Azure OAuth authority")?;
        if authority.scheme() != "https"
            || authority.host_str() != Some("login.microsoftonline.com")
        {
            return Err("Unsupported Azure OAuth authority");
        }
        let tenant = authority
            .path_segments()
            .and_then(|mut s| s.next())
            .filter(|s| !s.is_empty())
            .ok_or("Missing Azure OAuth tenant")?;
        let secret = scoped(cloud, "fs.azure.account.oauth2.client.secret", host)
            .ok_or("Missing Azure OAuth client secret")?;
        out.insert("azure_storage_tenant_id".into(), tenant.into());
        out.insert("azure_storage_client_secret".into(), secret.clone());
    }
    Ok(())
}
