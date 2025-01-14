# Deploy a StarRocks cluster via Web

1. Access the Web interface and configure a MySQL database for storing the management, query, and alerting information of Celerdata Manager. 

:::note
If you have multiple Celerdata clusters, we strongly recommend that you configure different MySQL accounts for different clusters to prevent unexpected issues caused by incorrect configurations.
:::

![img](../_assets/manager/003.png)

1. After the configuration is complete, click **Test Connection.** If the test is successful (**OK** is displayed at the top of the page), click **Next**.

![img](../_assets/manager/004.jpeg)

1. Specify the nodes to deploy, and the installation directories of **Agent** and **Supervisor**. Enter the internal network IP addresses for **Host IP** and use the default values for other parameters.

- **Host IP**: You can configure multiple IP addresses at a time. Separate multiple IP addresses with semicolons (;).
- Supervisor is used to manage the start and stop of processes.
- Agent is responsible for collecting statistical information of the machine.

All the installations are performed in the user environment and will not affect system environment.

![img](../_assets/manager/005.png)

**Notes**

- The system has two types of **Supervisors**: one is used to manage Agent, BE, FE, and Broker; the other is used to manage **Web** and **Center service**. 
- If you want to deploy Agent, FE, BE, and Broker on the same machine where **Web** and **Center service** are deployed, check whether Supervisor ports conflict with an existing port. If there is a conflict, perform the following operations: Modify the previously configured `bin/install.sh -s ${new_port}` to specify the Supervisor port required by Celerdata Manager, and make sure that all the Supervisors and Agents that manage FE, BE, and broker use the default ports.  

1. Click **Next**. In the displayed dialog box, select **Deploy a new Cluster** or **Migrate from an** **existed** **Cluster**. 

- If you have deployed FE and BE, but not Celerdata Manager, click **Migrate from an** **existed** **Cluster**.
- If this is your first-time deployment, that is, no FE/BE programs are running on your machine, click **Deploy a new Cluster**.

![img](../_assets/manager/006.png)
