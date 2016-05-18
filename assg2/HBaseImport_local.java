import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseImport {
  static final byte [] family = Bytes.toBytes("family");
  static final byte [] qualifier = Bytes.toBytes("qualifier");

   public static void main(String[] args) throws IOException {

     if (args.length != 1) {
       System.err.println("Usage: HBaseImport <directory>");
       return;
     }

     try {

       File dir = new File(args[0]);

       HBaseConfiguration hbaseConfig = new HBaseConfiguration();
       HTable hTable = new HTable(hbaseConfig, "ngram_in");

       int key = 0;
       File[] files = dir.listFiles();
       for (File file:files) {
         Scanner scan = new Scanner(new File(file.getAbsolutePath()));
         while(scan.hasNextLine()) {
           byte[] rowkey = Bytes.toBytes(Integer.toString(++key));
           String line = scan.nextLine();
           Put put = new Put(rowkey);
           put.add(family,qualifier,line.getBytes());
           hTable.put(put);
         }
       }

       hTable.flushCommits();
       hTable.close();

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
