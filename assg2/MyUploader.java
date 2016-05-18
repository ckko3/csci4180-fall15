/*
 * MyUploader.java
 *
 * How to run the program:
 * 
 * (i) Prepare the data file. For example, put a file called 'file01'
 * in the directory test-hbase/. The content of file01 looks something like:
 * 
 * 1,attribute,name,alice
 * 2,attribute,name,bob
 * 3,attribute,name,carol
 * 4,attribute,name,dave
 * 
 * (ii) Create the table in shell mode. The table must exist before you upload
 * the data. Let's call this table 'people'. From the above data, there must
 * be a corresponding column family called 'attribute'. So you can create the
 * table like this:
 * 
 * hbase(main):015:0> create 'people', 'attribute'
 * 0 row(s) in 1.1910 seconds
 * 
 * (iii) Compile and run the hbase program MyUploader.java. 
 * 
 * javac -classpath /usr/local/hbase/hbase-0.90.5.jar:/usr/local/hadoop/hadoop- core-0.20.203.0.jar MyUploader.java
 * hbase MyUploader test-hbase people
 * 
 */
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

public class MyUploader {

	public static class Uploader extends 
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		public void map(LongWritable key, Text line, Context context)
			throws IOException {
      
			// Input is a CSV file
			// Each map() is a single line, where the key is the line 
			// number. Each line is comma-delimited;
			// row,family,qualifier,value

			// Split CSV line
			String [] values = line.toString().split(",");
			if(values.length != 4) {
				return;
			}

			// Extract each value
			byte [] row = Bytes.toBytes(values[0]);
			byte [] family = Bytes.toBytes(values[1]);
			byte [] qualifier = Bytes.toBytes(values[2]);
			byte [] value = Bytes.toBytes(values[3]);

			// Create Put
			Put put = new Put(row);
			put.add(family, qualifier, value);

			// Uncomment below to disable WAL. This will improve
			// performance but means you will experience data loss in
			// the case of a RegionServer crash.
			// put.setWriteToWAL(false);

			try {
				context.write(new ImmutableBytesWritable(row), put);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
  
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "sampleuploader");

		job.setJarByClass(MyUploader.class);
		job.setMapperClass(Uploader.class);
		
		// No reducers.  Just write straight to table.  Call initTableReducerJob
		// because it sets up the TableOutputFormat.
		TableMapReduceUtil.initTableReducerJob(args[1], null, job);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.waitForCompletion(true);
	}
}
