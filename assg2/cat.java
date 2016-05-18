package org.myorg;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class cat{
        public static void main (String [] args) throws Exception{
                try{
                        FileSystem fs = FileSystem.get(new Configuration());
                        FileStatus[] status = fs.listStatus(new Path("hdfs://vm1:54310/user/hadoop/input"));
                        for (int i=0;i<status.length;i++){
                                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                                String line;
                                line=br.readLine();
                                while (line != null){
                                        System.out.println(line);
                                        line=br.readLine();
                                }
                        }
                }catch(Exception e){
                        System.out.println("File not found");
                }
        }
}
