import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import parallelSP.*;

public class ParallelDijkstra {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Error: Not enough command line argument");
            System.exit(-1);
        }
        String source = args[0];
        int maxTry = Integer.parseInt(args[1]);
        String inPath = args[2];
        String outPath = args[3];

        FileSystem hdfs = FileSystem.get(new Configuration());

        Configuration conf = new Configuration();
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");
        conf.set("mapred.reduce.tasks", "3");

        ToolRunner.run(conf, new MapImporter(), new String[]{source, inPath});
        hdfs.delete(new Path("outtmp/_SUCCESS"), true);
        hdfs.delete(new Path("outtmp/_logs"), true);
        hdfs.rename(new Path("outtmp"), new Path("intmp"));

        for (int trialno=1;; trialno++) {
            int improved = ToolRunner.run(conf, new DistanceImprove(), new String[]{});
            hdfs.delete(new Path("intmp"), true);
            hdfs.delete(new Path("outtmp/_SUCCESS"), true);
            hdfs.delete(new Path("outtmp/_logs"), true);
            hdfs.rename(new Path("outtmp"), new Path("intmp"));
            if (improved == 0) break;
            if (maxTry != 0 && maxTry == trialno) break;
        }
        ToolRunner.run(new SPExporter(), new String[]{outPath});
        hdfs.delete(new Path("intmp"), true);
    }
}
