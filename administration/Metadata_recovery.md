# Metadata Recovery

You may need to manually recover your FE if any of the following happens:

- FE fails to start bdbje
- FE fails to synchronize with other FEs
- Can’t perform metadata write operation
- No MASTER found

## Recovery Principle

To manually recover FEs,  start a new MASTER using the metadata stored in the current `meta_dir`, and then add other FEs one by one.

## Recovery example

Please strictly follow the steps below:

1. Stop all FE processes. To avoid unanticipated problems, prevent anyone from accessing the data when the recovery is in progress.
2. Find the FE node with the latest metadata
    a. Back up all FE `meta_dir` directories first
    b. Usually, the metadata of the leader FE is up-to-date.
    c. make sure the metadata is up-to-date by checking the suffix of the `image.xxxx` file in `meta_dir/image` directory. The bigger the suffix, the newer the metadata is.
    d. `mate_dir path` can be found in `fe.conf`
    ! [8-1](. /assets/8-1.png)
    e. The `meta_dir` folder structure is as follows:
    ! [8-2](... /assets/8-2.png)
    f. Compare the suffixes of the `image.xxxx` file in the `image` directory to identify the node with the most recent metadata
    ! [8-3](. /assets/8-3.png)
g. Use this FE node with the most recent metadata for recovery. It is recommended to choose the FOLLOWER node for recovery if possible. Use `cat ROL` to see the node role.

3. The following operations are performed on the FE node with the most recent metadata selected by step 2.
    a. If the node is an `OBSERVER`, change `role=OBSERVER` to `role=FOLLOWER` in the `meta_dir/image/ROLE`file. (Recovering from an `OBSERVER`node can be tricky, which will be explained later.) If the node is a FOLLOWER, skip this step.
    b. Add `metadata_failure_recovery=true` in `fe.conf`. The `metadata_failure_recovery=true` means to clear the metadata of "bdbje". In this way, bdbje will no longer contact other FEs and start as a standalone FE. This parameter should be set to true only when restart is in session, and must be set to false after restart is complete. Otherwise, the metadata of bdbje will be cleared again when restart is initiated and the other FEs will not work properly.
    c. Run `sh bin/start_fe.sh --deamon` to start the FE. This FE will start as MASTER normally, you will see `transfer from XXXX to MASTER` in `fe.log`.
    d. Connect to this FE and execute import queries to check the access. If an error occurs, troubleshoot the FE logs and restart the FE.
    e. If no error occurs, you should be able to see all the FEs added to the cluster by `show frontends;`. The current FE is MASTER.
    f. IMPORTANT STEP. Remove `metadata_failure_recovery=true` from `fe.conf`, or set it to false, and restart this FE.
    If the recovery is done with the metadata of an OBSERVER node, `show frontends;` will show the current FE as `OBSERVER`, but `IsMaster` is shown as true. This inconsistency is because the "OBSERVER" record is in StarRocks' metadata, whereas the value of `IsMaster` is recorded in bdbje's metadata. This inconsistency will prevent future operations (e.g.,`load`) from being performed, so it is necessary to fix it by following the steps:
    g. Drop all FE nodes except for this "OBSERVER" one.
    h. Add a new FOLLOWER FE by`ADD FOLLOWER`, assuming that it is on hostA.
    i. Start a brand new FE on hostA and join the cluster with the `--helper` method.
    j. Run`show frontends;`t, you should see two FEs, one for the previous OBSERVER and one for the newly added FOLLOWER, and the OBSERVER is master.
    k. Ensure that the new FOLLOWER is working properly, and then re-execute the failover operation (step b to step f) with the metadata of this new FOLLOWER. (If the synchronization of IDs as shown in the figure is completed, that means the new FOLLOWER is working properly)
    ! [8-4](... /assets/8-4.png)
The purpose of these steps above is to manually create metadata of the FOLLOWER node, and then use the metadata to start the fault recovery again.

4. After step 3 is executed successfully, we will add the other FEs by `ALTER SYSTEM DROP FOLLOWER/OBSERVER`.

The above steps complete the recovery.
