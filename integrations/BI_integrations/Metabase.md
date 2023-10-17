# Metabase

Metabase 支持对 StarRocks 中的内部数据和外部数据进行查询和可视化处理。

进入 Metabase，然后按如下操作：

1. 在 Metabase 主页右上角，单击 **Settings** 图标并选择 **Admin settings**。

   ![Metabase - Admin settings](../../assets/Metabase/Metabase_1.png)

2. 在页面上方菜单栏选择 **Databases**。

3. 在 **Databases** 页面，单击 **Add database**。

   ![Metabase - Add database](../../assets/Metabase/Metabase_2.png)

4. 在弹出的页面上，配置数据库参数，然后单击 **Save**。

   参数配置需要注意以下几点：

   - **Database type**：选择 **MySQL** 作为数据库类型。
   - **Host** 和 **Port**: 根据使用场景输入主机和端口信息。
   - **Database name**：按照 `<catalog_name>.<database_name>` 格式输入数据库名称。在 3.2 版本以前，StarRocks 只支持 Internal Catalog 与 Metabase 的集成。自 3.2 版本起，StarRocks 支持 Internal Catalog 和 External Catalog 与 Metabase 的集成。
   - **Username** 和 **Password**：输入 StarRocks 集群用户的用户名和密码。

   其他参数与 StarRocks 无关，您可以根据实际需要填写。

   ![Metabase - Configure database](../../assets/Metabase/Metabase_3.png)
