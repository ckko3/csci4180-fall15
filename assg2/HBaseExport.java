import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseExport{
  static final byte [] family = Bytes.toBytes("result");
  static final byte [] qualifier = Bytes.toBytes("count");

   public static void main(String[] args) throws IOException {
      if (args.length != 1) {
        System.err.println("Usage: HBaseExport <theta>");
        return;
      }

      int theta = Integer.parseInt(args[0]);

      HBaseConfiguration hbaseConfig = new HBaseConfiguration();
      HTable htable = new HTable(hbaseConfig, "ngraminitial_result");

      Scan scan = new Scan();
      ResultScanner scanner = htable.getScanner(scan);
      Result r;
      while (((r = scanner.next()) != null)) {
          String key = new String (r.getRow());
          String valString = new String(r.getValue(family, qualifier));
          int count = Integer.parseInt(valString);
          if(count >= theta)
              System.out.println(key + " " + valString);
        }

      scanner.close();
      htable.close();
   }
}
