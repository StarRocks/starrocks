# License Your CelerData Cluster using RESTful API

After you have deployed your CelerData cluster, you must acquire the license and append it to the cluster for the right of use. Besides appending the license while deploying the cluster using the Manager, you can also append it using the RESTful API.

Follow the steps listed below to acquire and append the license using the RESTful API.

:::note

The following operations requires the `cluster_admin` role. For more information, see [Built-in Roles](../administration/user_privs/authorization/built_in_roles.md).

:::

## Step 1. Collect system information

Collect the system information from your cluster using the following API.

Syntax:

```Bash
curl -u <username>:<password> <fe_host>:<fe_http_port>/api/v1/license/system_info
```

- `username`: The username of the cluster user who has the `cluster_admin` role.
- `password`: The password of the cluster user.
- `fe_host`: The host name or IP address of the Leader FE node within your cluster.
- `fe_http_port`: The HTTP server port (`http_port`) of the Leader FE node within your cluster. The default value of FE `http_port` is `8030`.

Example:

```Plain
curl -u root:123456 127.0.0.1:8030/api/v1/license/system_info
```

The cluster will return the system information. For example:

```Json
{"d":"qvLDpaVK0KZyhSMxHA+LjGj1MsW6ZFwOG0oznz7RtuSyTU+pNNZZP4z+NjUYykJxCCtrDnCsym5dmO+74owt4k03TL46AuHS3Hxj9cwuJYY=", "cores":104}
```

## Step 2. Obtain license file

Obtain the license file for your cluster by contacting the CelerData Support team and sending them the system information you acquired earlier.

The team will then grant you the license file (`license.txt`).

## Step 3. Register cluster with license file

Append the license file to your cluster to register it using the following API.

Syntax:

```Bash
curl -u <username>:<password> -XPOST --location-trusted --data-binary @<path_to_license.txt> <fe_host>:<fe_http_port>/api/v1/license/register
```

`path_to_license.txt`: The relative path to the license file (`license.txt`). Make sure the OS user has the access to the file.

Example:

```Plain
// license.txt is under the current directory.
curl  -u root:123456 -XPOST --location-trusted --data-binary @license.txt 127.0.0.1:8030/api/v1/license/register
```

After licensing the cluster, you are then granted the permission for use.

## View registered license

You can use the following API to view the registered license of the cluster.

Syntax:

```Bash
curl -u <username>:<password> <fe_host>:<fe_http_port>/api/v1/license/list
```

Example:

```Plain
curl -u root:123456 127.0.0.1:8030/api/v1/license/list
```

The cluster will return the license it is appended to. For example:

```Json
[{"sign":"","hosts":"0198a29e-3aa0-79f6-ad9f-9aa048dc9278","cores":104,"expire":1755679735457}]
```
