# Install Celerdata Manager

1. Obtain **CelerData-EE-x.x.x.tar.gz** of the required version from your sales manager and decompress this package.

```bash
tar -zxvf Celerdata-EE-x.x.x.tar.gz
```

1. Install the Web UI and Center service**.**
   1.  Go to the decompression directory, run the **install_path** script to generate a web interface, and install Celerdata Manager.
   2.  **install_path** is the installation directory of Celerdata Manager on your machine. You can use the default path. If you need to install multiple Celerdata Managers on the same machine or need to customize the directory, you can modify the **installation directory** or **port** as needed**.**
   3. "Install multiple Celerdata Managers on the same machine" means you can deploy multiple Celerdata Managers on the same machine to manage different clusters.

![img](../_assets/manager/001.svg)


```bash
cd Celerdata-EE-x.x.x

sh bin/install.sh -h
```

Supported configurations in **install.sh:**

```bash
-[d install_path] install_path(default: /home/disk1/celerdata/Celerdata-manager-20200101)
-[y python_bin_path] python_bin_path(default: /usr/bin/python)
-[p admin_console_port] admin_console_port(default: 19321)
-[s supervisor_http_port] supervisor_http_port(default: 19320)
```

If you deploy multiple Celerdata Managers on one machine, you must specify the installation path for each Celerdata Manager. You can use cluster name or IP address to differentiate Celerdata Managers.

Example 1: Run the following command to modify the file directory and port of Celerdata Manager.

```bash
sh bin/install.sh -d /home/disk1/Celerdata-manager  -p 19125  -s 19025
```

The specified directory is generated after this command is successfully executed.

![img](../_assets/manager/002.png)

Example 2: The generated directory contains the following three files. This directory is the installation directory of **Center service**.

```bash
drwxrwxr-x 9  4096 Feb 17 17:37 center
-rwxrwxr-x 1   366 Feb 17 17:37 centerctl.sh
drwxrwxr-x 3  4096 Feb 17 17:37 temp_supervisor
```

After the installation, you can access the web interface.

- For local access, use localhost:19321.
- For external access via an IP address, use `http://192.168.x.x: 19321`. `19321` is the `admin_console_port` specified for `-p`. Default port: 19321. If a success message is displayed but you still cannot access the web UI, check whether the tunnel is enabled.
- If you need to disable the **Web** service and the **Supervisor** that manages the **Web** service (for example, the Supervisor port is occupied or other errors occur), or need to modify the **Web** port, you can run the **centerctl.sh** script:

```bash
cd Celerdata-manager-xxx
./centerctl.sh -i

# Go to the Supervisor interface.
help # View commands.
status  # Check the current service and commands such as stop and shutdown.

# You can also reinstall Celerdata Manager.
./centerctl.sh daemon
```
