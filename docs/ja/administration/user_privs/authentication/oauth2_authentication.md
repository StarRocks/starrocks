---
displayed_sidebar: docs
sidebar_position: 50
---

# OAuth 2.0 認証

このトピックでは、StarRocks で OAuth 2.0 認証を有効にする方法を説明します。

v3.5.0 以降、StarRocks は OAuth 2.0 を使用してクライアントアクセスの認証をサポートしています。Web UI と JDBC ドライバーに対して HTTP 経由で OAuth 2.0 認証を有効にできます。

StarRocks は、トークンと引き換えに認可コードを使用する [Authorization Code](https://tools.ietf.org/html/rfc6749#section-1.3.1) フローを使用します。一般的に、このフローは以下のステップを含みます。

1. StarRocks のコーディネーターがユーザーのブラウザを認可サーバーにリダイレクトします。
2. ユーザーは認可サーバーで認証を行います。
3. リクエストが承認されると、ブラウザは認可コードと共に StarRocks FE にリダイレクトされます。
4. StarRocks のコーディネーターが認可コードをトークンと交換します。

このトピックでは、StarRocks で OAuth 2.0 を使用してユーザーを手動で作成し、認証する方法を説明します。セキュリティインテグレーションを使用して StarRocks を OAuth 2.0 サービスと統合する方法については、[Authenticate with Security Integration](./security_integration.md) を参照してください。OAuth 2.0 サービスでユーザーグループを認証する方法については、[Authenticate User Groups](../group_provider.md) を参照してください。

## 前提条件

MySQL クライアントから StarRocks に接続したい場合、MySQL クライアントのバージョンは 9.2 以上である必要があります。詳細については、[MySQL official document](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html) を参照してください。

## OAuth 2.0 を使用したユーザーの作成

ユーザーを作成する際、認証方法を OAuth 2.0 として `IDENTIFIED WITH authentication_oauth2 AS '{xxx}'` を指定します。`{xxx}` はユーザーの OAuth 2.0 プロパティです。

構文:

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_oauth2 AS 
'{
  "auth_server_url": "<auth_server_url>",
  "token_server_url": "<token_server_url>",
  "client_id": "<client_id>",
  "client_secret": "<client_secret>",
  "redirect_url": "<redirect_url>",
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

プロパティ:

- `auth_server_url`: 認可 URL。OAuth 2.0 認可プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL。
- `token_server_url`: StarRocks がアクセストークンを取得する認可サーバーのエンドポイントの URL。
- `client_id`: StarRocks クライアントの公開識別子。
- `client_secret`: 認可サーバーで StarRocks クライアントを認証するために使用される秘密。
- `redirect_url`: OAuth 2.0 認証が成功した後にユーザーのブラウザがリダイレクトされる URL。この URL に認可コードが送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2` として設定する必要があります。
- `jwks_url`: JSON Web Key Set (JWKS) サービスの URL または `conf` ディレクトリ内のローカルファイルのパス。
- `principal_field`: JWT 内でサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一でなければなりません。
- `required_issuer` (オプション): JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT の発行者と一致する場合にのみ、JWT は有効と見なされます。
- `required_audience` (オプション): JWT 内のオーディエンス (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT のオーディエンスと一致する場合にのみ、JWT は有効と見なされます。

例:

```SQL
CREATE USER tom IDENTIFIED WITH authentication_oauth2 AS 
'{
  "auth_server_url": "http://localhost:38080/realms/master/protocol/openid-connect/auth",
  "token_server_url": "http://localhost:38080/realms/master/protocol/openid-connect/token",
  "client_id": "12345",
  "client_secret": "LsWyD9vPcM3LHxLZfzJsuoBwWQFBLcoR",
  "redirect_url": "http://localhost:8030/api/oauth2",
  "jwks_url": "http://localhost:38080/realms/master/protocol/openid-connect/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```

## JDBC クライアントからの OAuth 2.0 接続

StarRocks は MySQL プロトコルをサポートしています。MySQL プラグインをカスタマイズして、ブラウザログインメソッドを自動的に起動することができます。

以下は JDBC クライアントの例です。

```Java

/**
 * StarRocks 'authentication_oauth2_client' 認証プラグイン。
 */
public class AuthenticationOAuth2Client implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "authentication_oauth2_client";

    private Long connectionId = null;
    private String sourceOfAuthData = PLUGIN_NAME;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        connectionId = prot.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void setAuthenticationParameters(String user, String password) {
    }

    @Override
    public void setSourceOfAuthData(String sourceOfAuthData) {
        this.sourceOfAuthData = sourceOfAuthData;
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (!this.sourceOfAuthData.equals(PLUGIN_NAME) || fromServer.getPayloadLength() == 0) {
            // サーバーからのペイロードで何もできない場合、
            // このイテレーションをスキップし、Protocol::AuthSwitchRequest または Protocol::AuthNextFactor を待ちます。
            return true;
        }

        // OAuth2 認可プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL
        int authServerUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String authServerUrl =
                fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", authServerUrlLength);

        // StarRocks クライアントの公開識別子。
        int clientIdLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String clientId = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", clientIdLength);

        // OAuth2 認証が成功した後にリダイレクトされる URL。
        int redirectUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String redirectUrl = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", redirectUrlLength);

        // OAuth2 のコールバック URL に含める必要がある StarRocks の接続 ID
        long connectionId = this.connectionId;

        String authUrl = authServerUrl +
                "?response_type=code" +
                "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8) +
                "&redirect_uri=" + URLEncoder.encode(redirectUrl, StandardCharsets.UTF_8) + "?connectionId=" + connectionId +
                "&scope=openid";

        Desktop desktop = Desktop.getDesktop();
        try {
            desktop.browse(new URI(authUrl));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

        NativePacketPayload packet = new NativePacketPayload(StringUtils.getBytes(""));
        packet.setPosition(packet.getPayloadLength());
        packet.writeInteger(NativeConstants.IntegerDataType.INT1, 0);
        packet.setPosition(0);

        toServer.add(packet);
        return true;
    }
}

public class OAuth2Main {
    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Properties properties = new Properties();
        properties.setProperty("defaultAuthenticationPlugin", "AuthenticationOAuth2Client");
        properties.setProperty("authenticationPlugins", "AuthenticationOAuth2Client");
    }
}
```

## MySQL クライアントからの OAuth 2.0 接続

ユーザーのブラウザへの直接呼び出しが許可されていない場合があります。そのような場合、StarRocks はネイティブ MySQL クライアントまたはネイティブ MySQL JDBC ドライバーもサポートしています。このモードでは、StarRocks と直接接続を確立できますが、StarRocks で操作を行うことはできません。コマンドを実行すると URL が返され、この URL にアクセスして OAuth 2.0 認証プロセスを完了する必要があります。