# Understand column and row-level security

## Background

In real-world business scenarios, a table can consist of numerous rows and columns. Customers require a fine-grained access control mechanism that allows only specific roles to query specific data columns or rows, in order to protect sensitive data.

Part of column and row-level security functionalities can be achieved using views. However, views cannot be applied to multiple tables. If the column to mask appear in multiple tables, users have to create a separate view for each table, resulting in an explosion of view numbers. On the other hand, views are not flexible in delegating privileges. Users must assign view and table privileges to data administrators or platform maintenance personnel. This reduces the flexibility of privilege management and increases the workload of platform maintenance personnel.

## What are column and row-level security?

CelerData supports using **column masking** and **row access policies** to protect sensitive data from unauthorized access while allowing authorized users to access data at query runtime. Users can specify masking and filter conditions when they create a policy. Only data columns or data rows that match the conditions are returned. This helps achieve column-level data masking and row-level data filtering.

When users query a table with a policy applied, the query will be rewritten. It's like a temporary view is generated based on the policy and the original query is redirected from the table to the temporary view. This ensures that only masked data is returned, not all data.

CelerData does not modify sensitive data at query runtime, nor does it encrypt data when storing the data.

**Typical use scenarios**:

- **Scenario 1**: For sensitive data like client phone numbers, customers want different roles to have different access permissions on such data. For example, sales personnel need to see plaintext phone numbers to sell products, while data analysts only need to see the last four digits of the phone number. This scenario requires a column masking policy, where columns are dynamically masked based on the role in the current session.
- **Scenario 2**: Sales data tables consist of data from different regions and customers want each region's sales staff to view data only from their own region. This scenario requires a row access policy, where only data rows of the relevant region are returned based on the role in the current session and the region information.

The use cases for these two scenarios can be found in [Masking policies](./masking_policy.md) and [Row access policies](./row_access_policy.md).

Policies can be created once and applied to multiple tables. For example, you can create a masking policy for a sensitive field and then apply that policy to all tables that contain this sensitive field.

Policy creation and application are controlled by privileges such as CREATE and APPLY. Administrators can determine whether to delegate these privileges to certain departments based on business scenarios. This allows for flexible privilege management and consistent application of policies across multiple tables, ensuring that sensitive data is protected consistently and according to the defined policies. For more information about privilege management, see [Manage privileges for policies](./authorization/user_privs.md).

## Benefits

- **Ease of Use**
  - A policy can be created once and then applied to multiple tables, eliminating the need to create numerous views.
- **Easy modification**
  - A policy is easy to modify and can take effect immediately without having to re-apply the policy to each table.
- **Flexible access control, Segregation of Duties**
  - Column masking and row access policies enable Segregation of Duties (SoD), allowing for fine-grained control over data access. Data management privileges can be decoupled from platform maintenance team. The data owners or dedicated data security teams can determine which roles have access to data of which security levels.

Overall, column masking and row access policies provide flexibility, ease of management, and enhanced control over data access and security, ensuring that sensitive data is protected and accessed only by authorized individuals or roles.

## Objects of a policy

A policy can be applied to a table, view, materialized view, or external table. The SQL statements used to create, apply, alter, or drop a policy are similar for these objects.

## Impact scope of a policy

A policy affects all the queries that run against the column with the policy applied, including the SELECT, UPDATE, DELETE, CTAS, and INSERT INTO AS SELECT operations.

:::note
Please note that query results of the DML statements may vary in accordance with the policies applied to the user who submitted the statement.
:::

## Usage notes

- You can apply column masking and row access policies to tables, views, materialized views, and external tables. The statements are similar for these objects.
- Policy is a database-level concept but it can be referenced across databases and catalogs when you apply a policy. The format is `catalog.db.policy`. If no catalog is specified, the current catalog is used by default.
- A table can have multiple column masking policies and row access policies. However, one column cannot have multiple row access policies.
- Row access policy and column masking policy cannot be applied to the same column. This is to prevent row access from exposing masked column data.
- Policies cannot be passed on. For example, CTAS and CREATE TABLE LIKE operations do not inherit the policies of the original table.
- Policies do not affect query rewrite of materialized views. When you query a base table, the query can be rewritten through the materialized view, no matter whether the materialized view has a policy applied.
- Column masking and row access policies, in essence, are to add predicates and conditions to the original query, which may degrade the query efficiency. You can determine whether to tune a policy based on the result of the EXPLAIN ANALYZE statement. The tuning and troubleshooting methods are similar to query profile analysis.
