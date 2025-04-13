// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authentication;

public class OAuth2ResultMessage {
    public static String generateLoginSuccessPage(String status, String message, String username, String connectionId) {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"zh\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>StarRocks OAuth2</title>\n" +
                "    <style>\n" +
                "        body {\n" +
                "            display: flex;\n" +
                "            justify-content: center;\n" +
                "            align-items: center;\n" +
                "            height: 100vh;\n" +
                "            background-color: #0D0E11;\n" +
                "            margin: 0;\n" +
                "            font-family: 'Arial', sans-serif;\n" +
                "            color: #ffffff;\n" +
                "        }\n" +
                "        .container {\n" +
                "            background: #1A1B1F;\n" +
                "            padding: 60px 80px;\n" +
                "            border-radius: 16px;\n" +
                "            box-shadow: 0px 6px 20px rgba(0, 0, 0, 0.3);\n" +
                "            text-align: center;\n" +
                "            max-width: 600px;\n" +
                "        }\n" +
                "        .logo-text {\n" +
                "            font-size: 32px;\n" +
                "            font-weight: bold;\n" +
                "            color: #00A3FF;\n" +
                "            margin-bottom: 30px;\n" +
                "        }\n" +
                "        h1 {\n" +
                "            color: #00A3FF;\n" +
                "            font-size: 36px;\n" +
                "            margin-bottom: 15px;\n" +
                "        }\n" +
                "        h2 {\n" +
                "            color: #FFD700;\n" +
                "        }\n" +
                "        p {\n" +
                "            font-size: 20px;\n" +
                "            color: #B0B3B8;\n" +
                "            margin-bottom: 30px;\n" +
                "        }\n" +
                "        .info {\n" +
                "            font-size: 18px;\n" +
                "            background: #25262A;\n" +
                "            padding: 16px;\n" +
                "            border-radius: 12px;\n" +
                "            margin-top: 15px;\n" +
                "            word-break: break-all;\n" +
                "            text-align: left;\n" +
                "        }\n" +
                "        .info strong {\n" +
                "            color: #00A3FF;\n" +
                "        }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div class=\"container\">\n" +
                "        <h1 class=\"logo-text\">StarRocks OAuth2 authentication</h1>\n" +
                "        <h2>" + status + "</h2>\n" +
                "        <p>" + message + "</p>\n" +
                "        <div class=\"info\">\n" +
                "            <strong>User : </strong> <span>" + username + "</span><br>\n" +
                "            <strong>ConnectionId : </strong> <span>" + connectionId + "</span>\n" +
                "        </div>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }
}
