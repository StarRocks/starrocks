// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class StreamLoadImportUtils {
    private static ExecutorService service;
    private static String sourceFilePath;
    private static String url;
    private static String auth;
    private static List<String> headers = new ArrayList<>();
    private static Integer size = 4096;
    private static Boolean isDebug = Boolean.FALSE;
    private static Integer connectTimeout = 60 * 1000;
    private static Integer threadNumber = Math.min(Runtime.getRuntime().availableProcessors(), 32);
    private static volatile BlockingDeque<String> blockingQueue = new LinkedBlockingDeque<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        resetDefaultConfig(args);
        printConfig();
        initWorkerThread();

        InputStream inputStream = new FileInputStream(sourceFilePath);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        StringBuilder stringBuilder = new StringBuilder();
        String line;
        int count = size;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line).append("\n");
            count--;
            if (count == 0) {
                blockingQueue.addLast(stringBuilder.toString());
                count = size;
                stringBuilder = new StringBuilder();
                // current-limiting
                while (blockingQueue.size() > size) {
                    Thread.sleep(30L);
                }
            }
        }
        // clear string builder
        if (stringBuilder.length() > 0) {
            blockingQueue.addLast(stringBuilder.toString());
        }
        // send signal to worker thread
        for (Integer i = 0; i < threadNumber; i++) {
            blockingQueue.addLast("");
        }

        try {
            reader.close();
            inputStreamReader.close();
            inputStream.close();
            service.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printConfig() {
        if (url == null) {
            System.out.println("url is empty , please set --url=xxx");
            System.exit(0);
        }
        if (auth == null) {
            System.out.println("auth is empty , please set --u=username:password");
            System.exit(0);
        }
        if (sourceFilePath == null) {
            System.out.println("source file path is empty , please set --source-file=/xxx/xx.csv");
            System.exit(0);
        }
        if (isDebug) {
            System.out.println(String.format("%s=%s", "sourceFilePath", sourceFilePath));
            System.out.println(String.format("%s=%s", "url", url));
            System.out.println(String.format("%s=%s", "size", size));
            System.out.println(String.format("%s=%s", "timeout", connectTimeout));
            System.out.println(String.format("%s=%s", "threadNumber", threadNumber));
            System.out.println(String.format("%s=%s", "auth", auth));
            System.out.println("Header:");
            for (String header : headers) {
                System.out.println(String.format("%s", header));
            }
        }
    }

    public static void executeGetAndSend() {
        OutputStream outputStream = null;
        InputStream inputStream = null;
        HttpURLConnection conn = null;
        try {
            // fe redirect be
            conn = getConnection(url);
            if (conn.getResponseCode() > 300 && conn.getResponseCode() < 400) {
                String redirectUrl = conn.getHeaderField("Location");
                conn.disconnect();
                conn = getConnection(redirectUrl);
            }

            // get data and send to be
            outputStream = conn.getOutputStream();
            String data;
            while ((data = blockingQueue.takeFirst()) != null) {
                if ("".equals(data)) {
                    break;
                }
                outputStream.write(data.getBytes(StandardCharsets.UTF_8));
            }

            inputStream = conn.getInputStream();
            int available = inputStream.available();
            byte[] bytes = new byte[available];
            inputStream.read(bytes);
            String result = new String(bytes);
            if (isDebug) {
                System.out.println(result);
            }
            if (result != null && result.contains("\"Status\": \"Fail\"")) {
                System.out.println("stream load status is fail \n" + result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static HttpURLConnection getConnection(String loadUrl) throws IOException {
        URL url = new URL(loadUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setUseCaches(false);
        conn.setReadTimeout(connectTimeout);
        conn.setConnectTimeout(connectTimeout);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("label", UUID.randomUUID().toString());
        conn.setInstanceFollowRedirects(false);
        conn.setRequestProperty("Expect", "100-continue");
        conn.setRequestProperty("Content-Type", "multipart/form-data;");
        conn.setRequestProperty("Authorization", "Basic " + auth);
        conn.setRequestProperty("Connection", "Keep-Alive");
        conn.setRequestProperty("Accept", "*/*");
        conn.setRequestProperty("Accept-Encoding", "gzip, deflate");
        conn.setRequestProperty("Cache-Control", "no-cache");
        conn.setRequestProperty("Content-Type", "multipart/form-data;");
        conn.setChunkedStreamingMode(8192);
        for (String header : headers) {
            String[] split = header.split(":");
            if (split.length > 1) {
                conn.setRequestProperty(split[0], split[1]);
            }
        }
        conn.connect();
        return conn;
    }

    public static void resetDefaultConfig(String[] args) {
        for (String arg : args) {
            String param = arg.replace("--", "");
            String name = param.substring(0, param.indexOf("="));
            String value = param.substring(param.indexOf("=") + 1);
            switch (name) {
                case "url":
                    url = value;
                    break;
                case "thread-number":
                    threadNumber = Integer.valueOf(value);
                    break;
                case "size":
                    size = Integer.valueOf(value);
                    break;
                case "is-debug":
                    isDebug = Boolean.valueOf(value);
                    break;
                case "timeout":
                    connectTimeout = Integer.valueOf(value);
                    break;
                case "u":
                    auth = new String(Base64.getEncoder().encode(value.getBytes(StandardCharsets.UTF_8)));
                    break;
                case "source-file":
                    sourceFilePath = value;
                    break;
                case "H":
                    headers.add(value);
                    break;
            }
        }
    }

    public static void initWorkerThread() {
        service = Executors.newFixedThreadPool(threadNumber);
        for (Integer i = 0; i < threadNumber; i++) {
            service.submit(() -> executeGetAndSend());
        }
    }
}
