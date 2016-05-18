package ngram;

import ngram.lib.JobBuilder;
import ngram.lib.NgramParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

public class NgramInitialRF extends Configured implements Tool {

    public static class NgramInitialRFMapper extends 
        Mapper<LongWritable, Text, Text, MapWritable> {

        private MapWritable stripe = new MapWritable();
        private NgramParser parser;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            parser.addLine(value.toString());
            List<Character> elms = null;
            while ((elms = parser.next()) != null) {
                stripe.clear();
                StringBuilder head = new StringBuilder();
                StringBuilder tail = new StringBuilder();
                for (char elm: elms) {
                    head.append(elm);
                    break;
                }
                for (char elm: elms) {
                   tail.append(elm);
                   tail.append(" ");
                }
                parser.shift();
                tail.deleteCharAt(tail.length()-1);
                String headstr = head.toString();
                String tailstr = tail.toString();
                stripe.put(new Text(tailstr), new IntWritable(1));
                context.write(new Text(headstr), stripe);
                }
            }

        protected void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int adjWordCount = conf.getInt("adjWordCount", 2);
            parser = new NgramParser(adjWordCount);
        }

//        protected void cleanup(Context context)
  //              throws IOException, InterruptedException {

//            for (Map.Entry<String, Integer> entry : NgramMap.entrySet()) {
  //              context.write(new Text(entry.getKey()),
    //                          new IntWritable(entry.getValue()));
   //         }
   //     }
    }

    public static class NgramInitialRFReducer extends 
        Reducer<Text, MapWritable, Text, DoubleWritable> {

        private MapWritable sumstripe = new MapWritable();

        public void reduce(Text key, Iterable<MapWritable> values,
                Context context) throws IOException, InterruptedException {        

            Configuration conf = context.getConfiguration();
            double theta = Double.parseDouble(conf.get("theta"));

            sumstripe.clear();

            for (MapWritable value : values){
                addstripe(value);
            }

            int total = 0;;
            for (Map.Entry<Writable, Writable> entry : sumstripe.entrySet()){
                total += Integer.parseInt(entry.getValue().toString());
            }

            for (Map.Entry<Writable, Writable> entry : sumstripe.entrySet()){
                StringBuilder sb = new StringBuilder();
                String out = sb.append(entry.getKey().toString()).toString();
                int current = Integer.parseInt(entry.getValue().toString());
                double rf = (double)current / (double)total;
                context.write(new Text(out), new DoubleWritable(rf));
            }
        }

        private void addstripe(MapWritable map){
            Set<Writable> allkey = map.keySet();
            for (Writable key : allkey) {
                IntWritable keyin = (IntWritable) map.get(key);
                if (sumstripe.containsKey(key)) {
                    IntWritable keyout = (IntWritable) sumstripe.get(key);
                    keyout.set(keyout.get() + keyin.get());
                } else {
                    sumstripe.put(key, keyin);
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length < 4) {
            JobBuilder.printUsage(this, "<input> <output> <number of adjacent words> <theta>");
            return -1;
        }
        conf.setInt("adjWordCount", Integer.parseInt(args[2]));
	    conf.set("theta", args[3]);
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);

        job.setMapperClass(NgramInitialRFMapper.class);
        job.setReducerClass(NgramInitialRFReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NgramInitialRF(), args);
        System.exit(exitCode);
    }
}
