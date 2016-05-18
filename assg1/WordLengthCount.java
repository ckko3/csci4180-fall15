package ngram;

import ngram.lib.JobBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configured;

public class WordLengthCount extends Configured implements Tool {

    public static class WordLengthCountMapper extends 
        Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private HashMap<Integer, Integer> wordLenMap = new HashMap<>();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                int len = word.length();
                int count = wordLenMap.containsKey(len) ? wordLenMap.get(len) : 0;
                wordLenMap.put(len, count+1);
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Map.Entry<Integer, Integer> entry : wordLenMap.entrySet()) {
                context.write(new IntWritable(entry.getKey()),
                              new IntWritable(entry.getValue()));
            }
        }
    }

    public static class WordLengthCountReducer extends 
        Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

        job.setMapperClass(WordLengthCountMapper.class);
        job.setReducerClass(WordLengthCountReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordLengthCount(), args);
        System.exit(exitCode);
    }
}
