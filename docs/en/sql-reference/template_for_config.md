---
displayed_sidebar: docs
unlisted: true
---

# Template for writing FE/BE parameters and variables

> When you add, modify, or delete an FE/BE parameter or a variable in code, do remember to update the documentation. [FE configuration](https://docs.starrocks.io/docs/administration/management/FE_configuration/), [BE configuration](https://docs.starrocks.io/docs/administration/management/BE_configuration/), [System variables](https://docs.starrocks.io/docs/reference/System_variable/).

The parameter or variable description usually contains the following fields:

- Default: *the default value of this parameter. If the default value varies in different versions, clearly specify this information.*
- Type: *the allowed data type*
- Unit: *the unit*
- Value range: *required if this parameter has a value range*
- Is mutable: *whether this parameter can be dynamically modified*
- Description: *the description of this parameter, including its meaning, valid values, dependency, risks, tuning method, disuse statement.*
- Introduced in: *since which version this parameter is introduced, accurate to 3-digit version.*

## Description of a parameter

### Meaning

- What is this parameter used for, in which scenario it is used.
- If this parameter is similar to an existing parameter, what is their difference?

### Dependency

- Whether this parameter depends on other parameters (that is, this parameter cannot take effect if other parameters are not set or enabled)
- Whether this parameter is a prerequisite for other parameters
- Whether this parameter is mutually exclusive to other parameters

### Valid values

If this parameter has multiple values, explain each value.

### Risks

- Whether there are any risks if this parameter is used
- If this parameter has a value range, what happens if the upper/lower threshold is exceeded
- what happens if this parameter is set to a value too large or too small

### Tuning method

When an error occurs, how to tune this parameter.

### Disuse statement

If this parameter is deleted:

- Explain the reason why it is deleted
- Whether it is replaced by another parameter
- The features that will also be deleted with this parameter

## Default value/unit change

If the default value or unit of a parameter is changed, explain why it is changed.

## Parameter name change

- Why the parameter name is changed
- Will there be compatibility issues? How the system deals with compatibility issues.
