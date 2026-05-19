---
displayed_sidebar: docs
sidebar_position: 45
---

# Manage permissions with Open Policy Agent

[Open Policy Agent](https://www.openpolicyagent.org/) (OPA) is a policy engine that can centralize authorization decisions for multiple systems. StarRocks can delegate privilege checks to OPA while continuing to use its existing authentication, group provider, analyzer, row access, and column masking machinery.

StarRocks sends OPA a StarRocks-native JSON payload. The policy response controls whether an action is allowed and can optionally return SQL expressions for row filters and column masks.

## Capabilities

- Authorize StarRocks objects including systems, catalogs, databases, tables, columns, views, materialized views, functions, resources, resource groups, storage volumes, pipes, and warehouses.
- Apply row access policies by returning SQL filter expressions.
- Apply column masking policies by returning SQL masking expressions.
- Use batch column masking to reduce OPA round trips for queries that reference many columns.

OPA authorization is fail-closed. StarRocks denies the operation if the OPA service returns `false`, omits `result`, returns invalid JSON, returns a non-2xx HTTP status, or times out.

## Configure OPA access control

Add the following items to the FE configuration file **fe.conf** on every FE node:

```properties
access_control = opa
opa_policy_url = http://opa.example.com:8181/v1/data/starrocks/allow

# Optional policy endpoints.
opa_row_filters_url = http://opa.example.com:8181/v1/data/starrocks/row_filters
opa_column_masking_url = http://opa.example.com:8181/v1/data/starrocks/column_mask
opa_batch_column_masking_url = http://opa.example.com:8181/v1/data/starrocks/batch_column_masks
```

Restart all FE nodes after changing `access_control` or any `opa_*` configuration item.

You can also enable OPA for an External Catalog by setting the catalog property `"catalog.access.control" = "opa"`. If this property is not set, the catalog uses the global `access_control` value.

```sql
CREATE EXTERNAL CATALOG hive_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
    "catalog.access.control" = "opa"
);
```

## Authorization request

StarRocks sends a POST request to `opa_policy_url` using the OPA Data API format:

```json
{
  "input": {
    "context": {
      "user": "alice",
      "groups": ["finance"],
      "host": "%",
      "queryId": "5e4b8e2d-2c80-4c49-94c2-1d8d7c11f8cc",
      "catalog": "default_catalog",
      "database": "sales"
    },
    "action": {
      "operation": "check",
      "privilege": "SELECT",
      "objectType": "TABLE",
      "resource": {
        "catalog": "default_catalog",
        "database": "sales",
        "table": "orders"
      }
    }
  }
}
```

Return `{"result": true}` to allow the request. Any other result denies it.

Example Rego policy:

```rego
package starrocks

default allow := false

allow if {
    input.action.privilege == "SELECT"
    input.action.objectType == "TABLE"
    input.action.resource.database == "sales"
    input.context.user == "alice"
}
```

## Row filters

When `opa_row_filters_url` is set, StarRocks asks OPA for row access expressions before query rewrite:

```json
{
  "result": [
    {"expression": "region = 'EMEA'"},
    {"expression": "tenant_id = 100"}
  ]
}
```

StarRocks combines multiple returned expressions with `AND`. Expressions must be valid StarRocks SQL expressions.

## Column masks

When `opa_column_masking_url` is set, StarRocks asks OPA for each column mask:

```json
{
  "result": {
    "expression": "NULL"
  }
}
```

When `opa_batch_column_masking_url` is set, StarRocks uses it instead of per-column requests:

```json
{
  "result": [
    {"column": "phone", "expression": "NULL"},
    {"column": "email", "expression": "concat('***@', split_part(email, '@', 2))"}
  ]
}
```

Expressions must be valid StarRocks SQL expressions. Invalid row filter or mask expressions fail the query.

## FE configuration items

For all OPA-related FE configuration items, see [FE configuration - User, role, and privilege](../../management/FE_parameters/user_query_loading.md#user-role-and-privilege).
