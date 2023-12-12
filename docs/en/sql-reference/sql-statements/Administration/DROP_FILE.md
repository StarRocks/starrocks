---
displayed_sidebar: "English"
---

# DROP FILE

## Description

You can execute the DROP FILE statement to delete a file. When you use this statement to delete a file, the file is deleted both in frontend (FE) memory and in Berkeley DB Java Edition (BDBJE).

:::tip

This operation requires the SYSTEM-level FILE privilege. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```SQL
DROP FILE "file_name" [FROM database]
[properties]
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | Yes          | The name of the file.                                        |
| database      | No           | The database to which a file belongs.                        |
| properties    | Yes          | The properties of the file. The following table describes the configuration items of properties. |

**Configuration items of** **`properties`**

| **Configuration items** | **Required** | **Description**                       |
| ----------------------- | ------------ | ------------------------------------- |
| catalog                 | Yes          | The category to which a file belongs. |

## Examples

Delete a file named **ca.pem**.

```SQL
DROP FILE "ca.pem" properties("catalog" = "kafka");
```
