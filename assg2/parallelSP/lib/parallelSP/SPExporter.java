package parallelSP;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class SPExporter extends Configured implements Tool {

    public static class NodeMapper extends Mapper<IntWritable, NodeWritable, IntWritable, IntWritable> {
        public void map(IntWritable id, NodeWritable node, Context context)
                throws IOException, InterruptedException {

            if (node.distance.get() != Integer.MAX_VALUE) {
                context.write(id, node.distance);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setMapperClass(NodeMapper.class);
        job.setJarByClass(SPExporter.class);
        // set reduce task to zero to allow mapper directly writing to filesystem
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, new Path("intmp"));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.waitForCompletion(true);
        return 0;
    }
}
