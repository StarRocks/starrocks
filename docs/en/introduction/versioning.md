# Version naming

Purpose: To provide a unified and clear explanation of the current naming conventions for StarRocks software packages.

## Numbering

The version numbering format for released versions is in the form of `MAJOR.MINOR.PATCH[-PRERELEASE]`, where PRERELEASE indicates the type and number of the prerelease version. Detailed explanations are as follows:

### MAJOR

The major version is incremented when incompatible API changes are made or there are major adjustments to the product's overall strategy.

### MINOR

The minor version is incremented when functionality is added in a backward-compatible manner. This typically refers to the addition of new features or improvements that do not break the API of the current major version or the main behavioral patterns of the software.

### PATCH

The patch version is incremented when backward-compatible bug fixes are made. This is usually for bug fixes or minor improvements that do not affect the software's main functions or API.

### PRERELEASE

Adding a prerelease identifier to the version indicates that the build is for testing by early adopters. The prerelease identifier will indicate the version number of the prerelease, such as `rc01`, `rc02`, etc. Currently, the only prerelease tag in use is `rc` followed by a two-digit number such as `rc01`.

:::note
Release Candidates are only built for the initial minor and major versions. For example, `3.4.0-rc01` or `4.0.0-rc01`.
:::

## Version examples

- `3.4.0`: The official release version.
- `3.4.0-rc01`: The first prerelease version of `3.4.0`. For instance, version `3.3` also had a second prerelease version `3.3.0-rc02`.
- `3.4.1`, `3.4.2`: Bug fix versions.

### Software package naming rules

The naming convention for software packages is: `StarRocks-x.y.z[-rcxx]{-os}{-arch}.tar.gz`. (That is, the main form is `Product-version-os-arch`)
1. x.y.z: follows the three-digit version numbering rule. `-rcxx` is an optional prerelease version number, such as `-rc01`, `-rc02`.
2. OS includes: `ubuntu`, `centos`. Mandatory.
3. ARCH includes: `amd64`. Mandatory. (There is no community edition for `arm64` yet.)

#### Example software package names
1. `StarRocks-3.4.0-rc01-ubuntu-amd64.tar.gz`: The prerelease version of 3.4.0, built for Ubuntu Linux.
2. `StarRocks-3.4.1-centos-amd64.tar.gz`: The PATCH revision built for Red Hat Enterprise Linux / CentOS.

