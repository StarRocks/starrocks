# CREATE USER

## description

### Syntax

```SQL
CREATE USER
user_identity [auth_option]
[DEFAULT ROLE 'role_name']

user_identity:
    'user_name'@'host'

auth_option: {
    IDENTIFIED BY 'auth_string'
    IDENTIFIED WITH auth_plugin
    IDENTIFIED WITH auth_plugin BY 'auth_string'
    IDENTIFIED WITH auth_plugin AS 'auth_string'
}
```

1. CREATE USER 命令用于创建一个 StarRocks 用户。在 StarRocks 中，一个 user_identity 唯一标识一个用户。

2. user_identity 由两部分组成，user_name 和 host，其中 user_name 为用户名。host 标识用户端连接所在的主机地址。host 部分可以使用 % 进行模糊匹配。如果不指定 host，默认为 '%'，即表示该用户可以从任意 host 连接到 StarRocks。

3. auth_option指定用户的认证方式，目前支持mysql_native_password和authentication_ldap_simple

    如果指定了角色（ROLE），则会自动将该角色所拥有的权限赋予新创建的这个用户。如果不指定，则该用户默认没有任何权限。指定的 ROLE 必须已经存在。

## example

1. 创建一个无密码用户（不指定 host，则等价于 jack@'%'）

    ```SQL
    CREATE USER 'jack';
    ```

2. 创建一个有密码用户，允许从 '172.10.1.10' 登陆

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';
    ```

    或者

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

3. 为了避免传递明文，用例2也可以使用下面的方式来创建

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    或者

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    后面加密的内容可以通过PASSWORD()获得到,例如：

    ```sql
    SELECT PASSWORD('123456');
    ```

4. 创建一个ldap认证的用户

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple
    ```

5. 创建一个ldap认证的用户，并指定用户在ldap中的DN(Distinguished Name)

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com'
    ```

6. 创建一个允许从 '192.168' 子网登陆的用户，同时指定其角色为 example_role

    ```sql
    CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
    ```

7. 创建一个允许从域名 'example_domain' 登陆的用户

    ```sql
    CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '12345';
    ```

8. 创建一个用户，并指定一个角色

    ```sql
    CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
    ```

## keyword

CREATE, USER
