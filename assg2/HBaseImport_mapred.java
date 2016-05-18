import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HBaseImport {
  static final byte [] family = Bytes.toBytes("family");
  static final byte [] qualifier = Bytes.toBytes("qualifier");

	public static class Import extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    public static int rowkey;

		public void map(LongWritable key, Text line, Context context)
			throws IOException {

      rowkey++;
			String val = line.toString();

			byte [] row = Bytes.toBytes(rowkey);
			byte [] value = Bytes.toBytes(val);

			Put put = new Put(row);
			put.add(family, qualifier, value);

			try {
				context.write(new ImmutableBytesWritable(row), put);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
    if (args.length != 1) {
			System.err.println("Usage: HBaseImport <directory>");
			return;
		}

		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "hbaseimport");

		job.setJarByClass(HBaseImport.class);
		job.setMapperClass(Import.class);

		TableMapReduceUtil.initTableReducerJob("ngram_in", null, job);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.waitForCompletion(true);
	}
}
