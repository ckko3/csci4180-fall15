import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MySummaryJob {
  public static class MyMapper extends TableMapper<Text, IntWritable>
  {
    public static final byte[] CF = “data”.getBytes();
    public static final byte[] ATTR1 = “1”.getBytes();

    private final IntWritable ONE = new IntWritable(1);
    private Text text = new Text();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
      KeyValue[] keyValue= value.raw();
      for(int i=0;i<keyValue.length;i++)
      {
        SplitKeyValue splitKeyValue=keyValue[i].split();
        //String val = new String(value.getValue(splitKeyValue.getFamily(), splitKeyValue.getQualifier()));
        System.out.println(“Family – “+Bytes.toString(splitKeyValue.getFamily()));
        System.out.println(“Qualifier – “+Bytes.toString(splitKeyValue.getQualifier() ));
        System.out.println(“Key: ” + Bytes.toString(splitKeyValue.getRow())  + “, Value: ” +Bytes.toString(splitKeyValue.getValue()));

        text.set(Bytes.toString(splitKeyValue.getRow())+”|”+Bytes.toString(splitKeyValue.getValue())+”|”+Bytes.toString(splitKeyValue.getFamily())+”|”+Bytes.toString(splitKeyValue.getQualifier()));     // we can only emit Writables…
        
      }
      context.write(text, ONE);
    }
  }
  public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>
  {
    public static final byte[] CF = “data”.getBytes();
    public static final byte[] COUNT = “count”.getBytes();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int i = 0;
      for (IntWritable val : values) {
        i += val.get();
      }
      System.out.println(“i===”+i);

      String keyString=key.toString();
      System.out.println(“keyString===”+keyString);
      String[] keyStringArray=keyString.split(“\\|”);

      System.out.println(“===========”+keyStringArray.length);
      String row=keyStringArray[0];
      String value=keyStringArray[1];
      String family=keyStringArray[2];
      String qualifier=keyStringArray[3];

      Put put = new Put(Bytes.toBytes(row));
      put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
      System.out.println(“Family – “+family);
      System.out.println(“Qualifier – “+qualifier);
      System.out.println(” Key: ” + row  + “, Value: ” +value);

      context.write(null, put);
    }
  }
  public static void main(String[] args) throws Exception
  {
    Configuration config = HBaseConfiguration.create();
    Job job = new Job(config,”ExampleSummary”);
    job.setJarByClass(MySummaryJob.class);     // class that contains mapper and reducer

    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don’t set to true for MR jobs
    // set other scan attrs

    TableMapReduceUtil.initTableMapperJob(
    “test”,        // input table
    scan,               // Scan instance to control CF and attribute selection
    MyMapper.class,     // mapper class
    Text.class,         // mapper output key
    IntWritable.class,  // mapper output value
    job);
    TableMapReduceUtil.initTableReducerJob(
    “target”,        // output table
    MyTableReducer.class,    // reducer class
    job);
    job.setNumReduceTasks(1);   // at least one, adjust as required

    boolean b = job.waitForCompletion(true);
    if (!b) {
      throw new IOException(“error with job!”);
    }

  }
}
