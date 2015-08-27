package com.microsoft.dsoap.misctest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by t-jamth on 8/6/2015.
 */
public class DsoapReadTest {

    public static void main(final String[] args) throws IOException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(Integer.parseInt(args[3]));
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
            final String term = (i % 2 == 0) ? args[0] : args[1];
            es.submit(new Callable<Void>() {
                public Void call() throws IOException {
                    // TODO fill in hostname
                    String query = "{\"Operation\": 2, \"Search\" : [{\"IdxStoreName\" : \"Text\", \"Query\" : \"" +
                            term + "\"}], \"Filter\" : [], \"Description\" : \"query from spark\"}";
                    String url = "http://XXX:20001/Query?Request=" +
                            URLEncoder.encode(query, "utf-8") + "&UseStreamResponse=true";
                    URL u = new URL(url);
                    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
                    conn.setRequestMethod("GET");
                    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                    int count = 0;
                    try {
                        while (br.readLine() != null)
                            count++;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(term + ": " + count);
                    // Thread.sleep(Integer.parseInt(args[4]));
                    return null;
                }
            });
        }
        es.shutdown();
        es.awaitTermination(1000, TimeUnit.DAYS);
    }
}
