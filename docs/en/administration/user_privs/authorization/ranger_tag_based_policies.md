---
displayed_sidebar: docs
sidebar_position: 41
---

# Tag-Based Access Control with Apache Ranger

StarRocks supports tag-based access control policies through its Apache Ranger integration. Tag-based policies allow administrators to define access rules based on data classification tags (e.g., "PII", "SENSITIVE", "CONFIDENTIAL") rather than individual resources, dramatically reducing policy management overhead at scale.

## Overview

With resource-based policies, protecting 500 tables containing PII data requires creating and maintaining 500 separate policies. Tag-based policies reduce this to a single policy: "Tag: PII -> Mask for Analysts" automatically applies to all resources tagged as PII.

Tag-based policies support:
- **Access control**: Allow or deny access based on tags
- **Column masking**: Automatically mask tagged columns (e.g., nullify PII fields)
- **Row filtering**: Apply row-level filters based on tags

## Prerequisites

- StarRocks with Ranger integration enabled (`access_control = ranger` in `fe.conf`)
- Apache Ranger Admin 2.1.0 or later
- Ranger service definition for StarRocks registered in Ranger Admin

## Setup

### Step 1: Create a Tag Service in Ranger Admin

1. Log into Ranger Admin UI
2. Navigate to **Settings** > **Service Manager**
3. Click **+** next to **TAG** to create a new tag service
4. Configure the tag service:
   - **Service Name**: `starrocks_tags` (or your preferred name)
   - **Description**: Tag service for StarRocks data classification

### Step 2: Associate the Tag Service with StarRocks

1. In Ranger Admin, navigate to the StarRocks service configuration
2. Edit the StarRocks service
3. In the **Tag Service** field, select the tag service created in Step 1 (e.g., `starrocks_tags`)
4. Save the configuration

After association, the StarRocks Ranger plugin automatically fetches tag policies alongside resource-based policies during its regular policy polling cycle.

### Step 3: Define Tags

Tags can be created in Ranger Admin or synchronized from Apache Atlas.

**Option A: Manual tag creation via Ranger Admin REST API**

Create a tag definition:

```bash
curl -u admin:password -X POST \
  http://<ranger-admin>:6080/service/tags/tagdef \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "PII",
    "source": "manual",
    "attributeDefs": [
      {
        "name": "data_classification",
        "type": "string"
      }
    ]
  }'
```

**Option B: Apache Atlas integration**

If your organization uses Apache Atlas for metadata governance, tags defined in Atlas are automatically synchronized to Ranger via the Atlas-Ranger integration. No additional StarRocks configuration is needed.

### Step 4: Tag Resources

Associate tags with StarRocks resources via the Ranger Admin REST API:

```bash
# Create a service resource (a StarRocks table)
curl -u admin:password -X POST \
  http://<ranger-admin>:6080/service/tags/resource \
  -H 'Content-Type: application/json' \
  -d '{
    "serviceName": "starrocks",
    "resourceElements": {
      "catalog": { "values": ["default_catalog"] },
      "database": { "values": ["customer_db"] },
      "table": { "values": ["customers"] }
    }
  }'

# Associate the PII tag with the resource
curl -u admin:password -X POST \
  http://<ranger-admin>:6080/service/tags/tagresourcemap \
  -H 'Content-Type: application/json' \
  -d '{
    "tagId": <tag_id>,
    "resourceId": <resource_id>
  }'
```

Tags can be applied at any level of the resource hierarchy:
- **Catalog level**: Tag applies to all databases, tables, and columns in the catalog
- **Database level**: Tag applies to all tables and columns in the database
- **Table level**: Tag applies to all columns in the table
- **Column level**: Tag applies to the specific column

### Step 5: Create Tag-Based Policies

In Ranger Admin, create policies under the tag service:

**Access control policy example:**

1. Navigate to the tag service (e.g., `starrocks_tags`)
2. Click **Add New Policy**
3. Configure:
   - **Policy Name**: Deny PII Access for Analysts
   - **TAG**: PII
   - **Deny Conditions**: Users in group `analysts`, access type `select`

**Data masking policy example:**

1. Navigate to the tag service
2. Click **Add New Policy** under **Masking** tab
3. Configure:
   - **Policy Name**: Mask PII Columns
   - **TAG**: PII
   - **Mask Conditions**: Users in group `analysts`, mask type `Nullify`

**Row filtering policy example:**

1. Navigate to the tag service
2. Click **Add New Policy** under **Row Level Filter** tab
3. Configure:
   - **Policy Name**: Filter Confidential Rows
   - **TAG**: CONFIDENTIAL
   - **Filter Conditions**: Users in group `analysts`, filter expression `sensitivity_level < 3`

## How It Works

The StarRocks Ranger plugin uses the standard Apache Ranger SDK (`RangerBasePlugin`), which handles tag-based policy evaluation internally:

1. The plugin periodically polls Ranger Admin for policy updates
2. Ranger Admin returns both resource-based and tag-based policies, along with tag-resource associations
3. The `RangerTagEnricher` (automatically configured by Ranger Admin) enriches each access request with the tags associated with the requested resource
4. The Ranger policy engine evaluates both tag-based and resource-based policies
5. Tag-based policies are evaluated first, resource-based policies can override

No StarRocks code changes are required. The `isAccessAllowed()`, `evalDataMaskPolicies()`, and `evalRowFilterPolicies()` methods in the Ranger SDK already evaluate both policy types.

## Policy Precedence

When both tag-based and resource-based policies apply to a resource:

1. Tag-based policies are evaluated first
2. Resource-based policies are evaluated second
3. A resource-based policy with equal or higher priority can override a tag-based decision
4. Deny policies take precedence over allow policies at the same priority level

## Supported Resource Types

Tag-based policies can be applied to the following StarRocks resource types:

| Resource Type | Tag Level | Notes |
|---|---|---|
| Catalog | Catalog | Tags cascade to all contained databases/tables |
| Database | Catalog > Database | Tags cascade to all contained tables |
| Table | Catalog > Database > Table | Most common tagging level |
| Column | Catalog > Database > Table > Column | For column-level data classification |

## Limitations

- Tags on views, materialized views, functions, and other non-table resources are not supported by the Ranger Tag Service. Use resource-based policies for these object types.
- The `root` user bypasses all Ranger policies, including tag-based policies.
- Tag synchronization latency depends on the Ranger plugin polling interval (default: 30 seconds, configurable via `ranger.plugin.starrocks.policy.pollIntervalMs`).

## Troubleshooting

**Tag policies not being evaluated:**
- Verify the tag service is associated with the StarRocks service in Ranger Admin
- Check the Ranger plugin logs for tag download errors
- Verify the tag-resource associations are correctly configured

**Policy changes not taking effect:**
- Wait for the policy polling interval (default 30 seconds)
- Check the Ranger Admin audit logs to confirm the plugin is polling
- Restart the FE node to force an immediate policy refresh

**Masking not applied to tagged columns:**
- Verify the tag is associated with the correct column-level resource
- Check that the masking policy in the tag service matches the tag name exactly
- Verify the user is not the `root` user (who bypasses all policies)
