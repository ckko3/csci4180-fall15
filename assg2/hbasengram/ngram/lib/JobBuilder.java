// This file is modified from the book "Hadoop The Definitive Guide" by Tom White

package ngram.lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class JobBuilder {

    private final Class<?> driverClass;
    private final Job job;
    private final int extraArgCount;
    private final String extrArgsUsage;

    private String[] extraArgs;

    public JobBuilder(Class<?> driverClass) throws IOException {
        this(driverClass, 0, "");
    }

    public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) throws IOException {
        this.driverClass = driverClass;
        this.extraArgCount = extraArgCount;
        this.job = new Job();
        this.job.setJarByClass(driverClass);
        this.extrArgsUsage = extrArgsUsage;
    }

    public static Job parseInputAndOutput(Tool tool, Configuration conf,
            String[] args) throws IOException {
/*
        if (args.length < 1) {
            printUsage(tool, "<input> <output>");
            return null;
        }*/
        Job job = new Job(conf);
        job.setJarByClass(tool.getClass());
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n",
                tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }
}
