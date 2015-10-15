package ch.daplab.training.tp1.mapreduce.lib.output;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Inspirate from http://pastebin.com/mfpLXsB8 and ported to the new org.apache.hadoop.mapreduce API.
 */
public class StreamingTextOutputFormat<K, V> extends TextOutputFormat {

    public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
    public static String DELIMITER = "mapreduce.output.textoutputformat.delimiter";

    public StreamingTextOutputFormat() {
    }

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = conf.get(SEPERATOR, "\t");
        String valueDelimiter = conf.get(DELIMITER, ",");

        CompressionCodec codec = null;
        String extension = "";
        if(isCompressed) {
            Class file = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec)ReflectionUtils.newInstance(file, conf);
            extension = codec.getDefaultExtension();
        }

        Path file1 = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file1.getFileSystem(conf);
        FSDataOutputStream fileOut;
        if(!isCompressed) {
            fileOut = fs.create(file1, false);
            return new StreamingTextOutputFormat.StreamingLineRecordWriter(fileOut, keyValueSeparator, valueDelimiter);
        } else {
            fileOut = fs.create(file1, false);
            return new StreamingTextOutputFormat.StreamingLineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator, valueDelimiter);
        }
    }

    protected static class StreamingLineRecordWriter<K, V> extends RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;
        private final byte[] valueDelimiter;
        private boolean dataWritten = false;

        public StreamingLineRecordWriter(DataOutputStream out, String keyValueSeparator, String valueDelimiter) {
            this.out = out;

            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
                this.valueDelimiter = valueDelimiter.getBytes(utf8);
            } catch (UnsupportedEncodingException var4) {
                throw new IllegalArgumentException("can\'t find UTF-8 encoding");
            }
        }

        public StreamingLineRecordWriter(DataOutputStream out) {
            this(out, "\t", ",");
        }

        private void writeObject(Object o) throws IOException {
            if(o instanceof Text) {
                Text to = (Text)o;
                this.out.write(to.getBytes(), 0, to.getLength());
            } else {
                this.out.write(o.toString().getBytes("UTF-8"));
            }
        }

        public synchronized void write(K key, V value) throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;

            if (!nullKey) {
                // if we've written data before, append a new line
                if (dataWritten) {
                    out.write(newline);
                }

                // write out the key and separator
                writeObject(key);
                out.write(keyValueSeparator);
            } else if (!nullValue) {
                // write out the value delimiter
                out.write(valueDelimiter);
            }

            if (!nullValue) {
                // write out the value
                writeObject(value);
            }

            if (!nullKey || !nullValue) {
                // track that we've written some data
                dataWritten = true;
            }
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            // if we've written out any data, append a closing newline
            if (dataWritten) {
                out.write(newline);
            }

            this.out.close();
        }

        static {
            try {
                newline = "\n".getBytes("UTF-8");
            } catch (UnsupportedEncodingException var1) {
                throw new IllegalArgumentException("can\'t find UTF-8 encoding");
            }
        }
    }
}