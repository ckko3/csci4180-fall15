package parallelSP;

import java.io.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class DistanceImprove extends Configured implements Tool {
    static enum MyCounter { NEGDIST, POSDIST, IMPROVED };

    public static class NodeMapper extends Mapper<IntWritable, NodeWritable, IntWritable, NodeWritable> {
        public void map(IntWritable id, NodeWritable node, Context context)
                throws IOException, InterruptedException {
            int myDistance = node.distance.get();

            /* emit itself. Use positive value to make it special
             * Given that the edge weight greater or eq 0, it is safe to do so
             */
            context.write(id, node);

            // if itself is modified, emit distance to child
            if (!node.moded.get()) {
                return;
            }
            for (Writable[] edge: node.edges.get()) {
                int outid = ((IntWritable) edge[0]).get();
                int weight = ((IntWritable) edge[1]).get();
                if (weight == Integer.MAX_VALUE)
                    throw new RuntimeException("Encounter edge with INF distance");
                if (myDistance == Integer.MAX_VALUE)
                    throw new RuntimeException("Encounter node with INF distrance. id: " + id.toString());
                if (myDistance < 0)
                    throw new RuntimeException("Encounter node with neg distance. id: " + id.toString());
                context.write(new IntWritable(outid), new NodeWritable(new IntWritable(-(myDistance + weight))));
            }
        }
    }

    public static class NodeReducer extends 
        Reducer<IntWritable, NodeWritable, IntWritable, NodeWritable> {

        // code modified from Apache Crunch Core
        public static NodeWritable deepcopy(NodeWritable source) {
            ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();  
            DataOutputStream dataOut = new DataOutputStream(byteOutStream);  
            NodeWritable copiedValue = new NodeWritable();
            try {  
                source.write(dataOut);  
                dataOut.flush();  
                ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());  
                DataInput dataInput = new DataInputStream(byteInStream);  
                copiedValue.readFields(dataInput);
            } catch (Exception e) {
                throw new RuntimeException("Error copying writable");
            }
            return copiedValue;
        }

        @Override
        public void reduce(IntWritable id, Iterable<NodeWritable> vals, Context context)
                throws IOException, InterruptedException {
            int minDist = Integer.MAX_VALUE;
            NodeWritable origNode = null;
            for (NodeWritable x: vals) {
                int d = x.distance.get();
                if (d >= 0) {
                    origNode = deepcopy(x);
                    context.getCounter(MyCounter.POSDIST).increment(1);
                } else if (-d < minDist) {
                    minDist = -d;
                    context.getCounter(MyCounter.NEGDIST).increment(1);
                }
            }

            if (minDist < origNode.distance.get()) {
                origNode.distance = new IntWritable(minDist);
                origNode.moded = new BooleanWritable(true);
                context.getCounter(MyCounter.IMPROVED).increment(1);
            } else {
                origNode.moded = new BooleanWritable(false);
            }
            if (origNode.distance.get() < 0)
                throw new RuntimeException("emitting negative distance: " + origNode.distance.toString() + " " + id.toString());
            context.write(id, origNode);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(DistanceImprove.class);
        job.setMapperClass(NodeMapper.class);
        job.setReducerClass(NodeReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NodeWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, new Path("intmp"));
        SequenceFileOutputFormat.setOutputPath(job, new Path("outtmp"));

        job.waitForCompletion(true);
        return (int) job.getCounters().findCounter(MyCounter.IMPROVED).getValue();
    }
}
