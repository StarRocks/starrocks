# Deployment overview

This chapter describes how to deploy, upgrade, and downgrade a StarRocks cluster in a production environment.

## Deployment procedure

A summary of the deployment procedure is as follows and later topics provide the details.

The deployment of StarRocks generally follows the steps outlined here:

1. Confirm the [hardware and software requirements](../deployment/deployment_prerequisites.md) for your StarRocks deployment.

   Check the prerequisites that your servers must meet before deploying StarRocks, including CPU, memory, storage, network, operating system, and dependencies.

2. [Plan your cluster size](../deployment/plan_cluster.md).

   Plan the number of FE nodes and BE nodes in your cluster, and the hardware specifications of the servers.

3. [Check environment configurations](../deployment/environment_configurations.md).

   When your servers are ready, you need to check and modify some environment configurations before deploying StarRocks on them.

4. [Prepare deployment files](../deployment/prepare_deployment_files.md).

   - If you want to deploy StarRocks on x86-based CentOS 7.9, you can directly download and extract the software package provided on our official website.
   - If you want to deploy StarRocks with ARM architecture CPUs or on Ubuntu 22.04, you need to prepare the deployment files from the StarRocks Docker image.
   - If you want to deploy StarRocks on Kubernetes, you can skip this step.

5. Deploy StarRocks.

   - If you want to deploy a shared-data StarRocks cluster, which features a disaggregated storage and compute architecture, see [Deploy and use shared-data StarRocks](../deployment/deploy_shared_data.md) for instructions.
   - If you want to deploy a classic StarRocks cluster, which uses local storage, you have the following options:

     - [Deploy StarRocks manually](../deployment/deploy_manually.md).
     - [Deploy StarRocks on Kubernetes with operator](../deployment/sr_operator.md).
     - [Deploy StarRocks on Kubernetes with Helm](../deployment/helm.md).
     - [Deploy StarRocks on AWS](../deployment/starrocks_on_aws.md).

6. Perform necessary [post-deployment setup](../deployment/post_deployment_setup.md) measures.

   Further setup measures are needed before your StarRocks cluster is put into production. These measures include securing the initial account and setting some performance-related system variables.

## Upgrade and downgrade

If you plan to upgrade an existing StarRocks cluster to a later version rather than install StarRocks for the first time, see [Upgrade StarRocks](../deployment/upgrade.md) for information about upgrade procedures and issues that you should consider before upgrading.

For instructions to downgrade your StarRocks cluster, see [Downgrade StarRocks](../deployment/downgrade.md).
