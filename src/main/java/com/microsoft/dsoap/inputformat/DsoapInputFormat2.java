package com.microsoft.dsoap.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by t-jamth on 7/20/2015.
 */
public class DsoapInputFormat2 extends InputFormat<LongWritable, Text> {
    // TODO read the hosts from a config file or from the JobConf
    private static final String[] HOSTS = {};
    private static final int DATA_SERVER_PORT = 20001;

    private static final Map<Integer, Integer> MONTHS = new HashMap<Integer, Integer>();
    // one RDD partition created per split, one split per day bucket per datanode
    // (provides more parallelism than DsoapInputFormat if that is necessary)
    private static final List<InputSplit> SPLITS = new ArrayList<InputSplit>();

    static {
        MONTHS.put(11, 30);
        MONTHS.put(12, 31);

        for (String host : HOSTS) {
            for (Map.Entry<Integer, Integer> e : MONTHS.entrySet()) {
                for (int i = 1; i <= e.getValue(); i++) {
                    SPLITS.add(new DsoapInputSplit2(host, String.format("bucket-%02d-%02d", e.getKey(), i)));
                }
            }
        }
    }

    protected static class DsoapInputSplit2 extends InputSplit implements Writable {
        private String hostname;
        private String bucket;

        public DsoapInputSplit2(String hostname, String bucket) {
            this.hostname = hostname;
            this.bucket = bucket;
        }

        // for serialization
        public DsoapInputSplit2() {}

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
            return "DsoapInputSplit2[" + hostname + "," + bucket + "]";
        }

        public String getBucket() {
            return bucket;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(hostname);
            dataOutput.writeUTF(bucket);
        }

        public void readFields(DataInput dataInput) throws IOException {
            hostname = dataInput.readUTF();
            bucket = dataInput.readUTF();
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
            // query should have field "Bucket": "%s", and we substitute the appropriate bucket here
            // TODO we should be able to rewrite the query to include the bucket without this sort of
            // support from the client
            String query = String.format(c.get("dsoap.query"), ((DsoapInputSplit2) inputSplit).getBucket());

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
