# SHOW ROUTINE LOAD

## 功能

查看 routine load 任务的信息。

## 示例

1. 展示名称为 test1 的所有例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    ```sql
    SHOW ALL ROUTINE LOAD FOR test1;
    ```

2. 展示名称为 test1 的当前正在运行的例行导入作业

    ```sql
    SHOW ROUTINE LOAD FOR test1;
    ```

3. 显示 example_db 下，所有的例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    ```sql
    use example_db;
    SHOW ALL ROUTINE LOAD;
    ```

4. 显示 example_db 下，所有正在运行的例行导入作业

    ```sql
    use example_db;
    SHOW ROUTINE LOAD;
    ```

5. 显示 example_db 下，名称为 test1 的当前正在运行的例行导入作业

    ```sql
    SHOW ROUTINE LOAD FOR example_db.test1;
    ```

6. 显示 example_db 下，名称为 test1 的所有例行导入作业（包括已停止或取消的作业）。结果为一行或多行。

    ```sql
    SHOW ALL ROUTINE LOAD FOR example_db.test1;
    ```

## 关键字(keywords)

SHOW, ROUTINE, LOAD
