package ngram;

import ngram.lib.JobBuilder;
import ngram.lib.NgramParser;

import java.io.IOException
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HBaseNgramInitial extends Configured implements Tool {

    public static class MyMapper extends TableMapper<Text, IntWritable> {

      static final byte [] family_in = Bytes.toBytes("family");
      static final byte [] qualifier_in = Bytes.toBytes("qualifier");

        private HashMap<String, Integer> NgramMap = new HashMap<>();
        private NgramParser parser;

        public void map(ImmutableBytesWritable row, Result value,
         Context context) throws IOException, InterruptedException {

            String line = new String(value.getValue(family_in, qualifier_in));
            parser.addLine(line);
            List<Character> elms = null;
            while ((elms = parser.next()) != null) {
                StringBuilder sb = new StringBuilder();
                for (char elm: elms) {
                    sb.append(elm);
                    sb.append(" ");
                }
                parser.shift();
                sb.deleteCharAt(sb.length()-1);
                String gram = sb.toString();

                int count = NgramMap.containsKey(gram) ? NgramMap.get(gram) : 0;
                NgramMap.put(gram, count+1);
            }
        }

        protected void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int adjWordCount = conf.getInt("adjWordCount", 2);
            parser = new NgramParser(adjWordCount);
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : NgramMap.entrySet()) {
                context.write(new Text(entry.getKey()),
                              new IntWritable(entry.getValue()));
            }
        }
    }

    public static class MyTableReducer extends
         TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        static final byte [] family_out = Bytes.toBytes("result");
        static final byte [] qualifier_out = Bytes.toBytes("count");

        public void reduce(Text key, Iterable<IntWritable> values,
         Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(family_out, qualifier_out, Bytes.toBytes(Integer.toString(sum)));

            context.write(null, put);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length < 1) {
            JobBuilder.printUsage(this, "<number of adjacent words>");
            return -1;
        }

        conf.setInt("adjWordCount", Integer.parseInt(args[0]));
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);

        Configuration config = HBaseConfiguration.create();
        job = new Job(config, "HBaseNgramInitial");
        job.setJarByClass(HBaseNgramInitial.class);     // class that contains mapper and reducer

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don’t set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
        "ngram_in", // input table
        scan, // scan instance
        MyMapper.class, // mapper class
        Text.class, // mapper output key
        IntWritable.class, // mapper output value
        job);
        TableMapReduceUtil.initTableReducerJob(
        "ngraminitial_result", // output table
        MyTableReducer.class, // reducer class
        job);
        job.setNumReduceTasks(1); // at least one, adjusted as required
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseNgramInitial(), args);
        System.exit(exitCode);
    }
}
