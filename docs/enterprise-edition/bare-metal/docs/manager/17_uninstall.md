# Uninstall Celerdata Manager and StarRocks

1. Run the following commands in sequence on the directories where all nodes are located. 

    ```bash
    cd  celerdata-manager-20201102/agent
    ./agentctl.sh stop all
    ./agentctl.sh shutdown
    ```

1. Run the following commands on the machine where CelerData Manager is deployed. 

    ```bash
    cd  celerdata-manager-20201102/center
    ./centerctl.sh stop all
    ./centerctl.sh shutdown 
    ```

1. Delete (or back up) all the **celerdata-xxx** and **celerdata-manager-xxx** directories. If you use customized directories, stop all the processes managed by Supervisor and delete all the related directories.
