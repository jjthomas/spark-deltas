package com.microsoft.dsoap.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.yarn.webapp.WebApp;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by t-jamth on 7/20/2015.
 */
public class DsoapInputFormat extends InputFormat<LongWritable, Text> {
    // TODO read the hosts from a config file or from the JobConf
    private static final String[] HOSTS = {};
    private static final int DATA_SERVER_PORT = 20001;
    private static final int INDEX_SERVER_PORT = 20002;

    // one RDD partition created per split, one split per datanode
    private static final List<InputSplit> SPLITS = new ArrayList<InputSplit>();

    static {
        for (String host : HOSTS) {
            SPLITS.add(new DsoapInputSplit(host));
        }
    }

    // useful for testing to make sure we are doing fair comparisons
    public static void clearIndexCache() throws IOException {
        for (String host : HOSTS) {
            String url = "http://" + host + ":" + INDEX_SERVER_PORT + "/ClearCache";
            URL u = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            conn.getResponseCode();
            conn.disconnect();
        }
    }

    protected static class DsoapInputSplit extends InputSplit implements Writable {
        private String hostname;

        public DsoapInputSplit(String hostname) {
            this.hostname = hostname;
        }

        // for serialization
        public DsoapInputSplit() {}

        @Override
        public long getLength() throws IOException, InterruptedException {
            // not implemented
            return 1l;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[] { hostname };
        }

        @Override
        public String toString() {
            return "DsoapInputSplit[" + hostname + "]";
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(hostname);
        }

        public void readFields(DataInput dataInput) throws IOException {
            hostname = dataInput.readUTF();
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return SPLITS;
    }

    // to read the records in a split
    protected static class DsoapRecordReader extends RecordReader<LongWritable, Text> {
        private BufferedReader br;
        private HttpURLConnection conn;
        private LongWritable key = new LongWritable(1);
        private Text value = new Text();
        private String url;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Configuration c = taskAttemptContext.getConfiguration();
            String query = c.get("dsoap.query");
            url = "http://" + inputSplit.getLocations()[0] + ":" + DATA_SERVER_PORT + "/Query?Request=" +
                    URLEncoder.encode(query, "utf-8") + "&UseStreamResponse=true";
            URL u = new URL(url);
            conn = (HttpURLConnection) u.openConnection();
            conn.setRequestMethod("GET");
            br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            String line;
            do {
                line = br.readLine();
                if (line == null) {
                    return false;
                }
            } while (line.isEmpty());
            value.set(line);
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            // not implemented
            return 0;
        }

        @Override
        public void close() throws IOException {
            br.close();
            conn.disconnect();
        }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new DsoapRecordReader();
    }
}
