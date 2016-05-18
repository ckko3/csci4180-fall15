package parallelSP;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MapImporter extends Configured implements Tool {

    public static class ImportMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private Set<Integer> nodesSeen = new HashSet<>();

        public void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            String [] tokens = line.toString().split(" ");
            if(tokens.length != 3) {
                return;
            }

            // Extract each value
            int from = Integer.parseInt(tokens[0]);
            nodesSeen.add(from);
            nodesSeen.add(Integer.parseInt(tokens[1]));

            context.write(new IntWritable(from), line);
        }

        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            for (int id: nodesSeen) {
                context.write(new IntWritable(id), new Text(""));
            }
        }
    }

    public static class ImportReducer extends 
        Reducer<IntWritable, Text, IntWritable, NodeWritable> {

        public void reduce(IntWritable id, Iterable<Text> lines, Context context)
                throws IOException, InterruptedException {
            ArrayList<IntWritable[]> edges = new ArrayList<>();
            for (Text line: lines) {
                if (line.toString().equals("")) continue;
                String [] tokens = line.toString().split(" ");
                IntWritable to = new IntWritable(Integer.parseInt(tokens[1]));
                IntWritable weight = new IntWritable(Integer.parseInt(tokens[2]));
                edges.add(new IntWritable[] {to, weight});
            }

            IntWritable[][] outEdge = new IntWritable[edges.size()][2];
            for (int i=0; i<edges.size(); i++) {
                outEdge[i] = edges.get(i);
            }

            Configuration conf = context.getConfiguration();
            int sourceNode = Integer.parseInt(conf.get("sourceNode"));

            boolean moded = (sourceNode == id.get()) ? true: false;
            int distance = (sourceNode == id.get()) ? 0: Integer.MAX_VALUE;
            context.write(id, new NodeWritable(
                id, new IntWritable(distance), moded,
                new NodeWritable.EdgeWritables(outEdge)
            ));
        }
    }

    public int run(String[] args) throws Exception {
        String sourceNode = args[0];
        String inPath = args[1];

        Configuration conf = getConf();
        conf.set("sourceNode", sourceNode);
        Job job = new Job(conf);
        FileInputFormat.addInputPath(job, new Path(inPath));
        SequenceFileOutputFormat.setOutputPath(job, new Path("outtmp"));
        job.setJarByClass(MapImporter.class);
        job.setMapperClass(ImportMapper.class);
        job.setReducerClass(ImportReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NodeWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
        return 0;
    }
}
