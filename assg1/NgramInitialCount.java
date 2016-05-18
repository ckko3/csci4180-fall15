package ngram;

import ngram.lib.JobBuilder;
import ngram.lib.NgramParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

public class NgramInitialCount extends Configured implements Tool {

    public static class NgramInitialCountMapper extends 
        Mapper<LongWritable, Text, Text, IntWritable> {

        private HashMap<String, Integer> NgramMap = new HashMap<>();
        private NgramParser parser;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.addLine(value.toString());
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

    public static class NgramInitialCountReducer extends 
        Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length < 3) {
            JobBuilder.printUsage(this, "<input> <output> <number of adjacent words>");
            return -1;
        }
        conf.setInt("adjWordCount", Integer.parseInt(args[2]));
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);

        job.setMapperClass(NgramInitialCountMapper.class);
        job.setReducerClass(NgramInitialCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NgramInitialCount(), args);
        System.exit(exitCode);
    }
}
