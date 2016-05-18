package org.myorg;

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

       HBaseConfiguration hbaseConfig = new HBaseConfiguration();
       HTable hTable = new HTable(hbaseConfig, "ngram_in");

       FileSystem fs = FileSystem.get(new Configuration());
       FileStatus[] status = fs.listStatus(new Path("hdfs://vm1:54310/user/hadoop/" + args[0]));

       int key = 0;

       for (int i = 0; i < status.length; i++) {
         BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
         String line = br.readLine();
         while (line != null){
              byte[] rowkey = Bytes.toBytes(Integer.toString(++key));
              Put put = new Put(rowkey);
              put.add(family,qualifier,line.getBytes());
              hTable.put(put);
              line = br.readLine();
            }
          }

          hTable.flushCommits();
          hTable.close();

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
