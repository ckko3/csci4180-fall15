/* This file is modified from Hadoop the Definitive Guide 3rd edition */

package parallelSP;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class NodeWritable implements Writable {

    public static class EdgeWritables extends TwoDArrayWritable {
       public EdgeWritables() { super(IntWritable.class); }
       public EdgeWritables(IntWritable[][] edges) {
           this();
           this.set(edges);
       }
    }

    public IntWritable id;
    public IntWritable distance;
    public BooleanWritable moded;
    public EdgeWritables edges;

    public NodeWritable() {
        this(new IntWritable(9999), new IntWritable(9999), false, new EdgeWritables());
    }

    public NodeWritable(IntWritable distance) {
        if (distance.get() >= 0)
            throw new RuntimeException("Should set edge to negative " + distance.toString());
        this.distance = distance;
        this.id = new IntWritable(999);
        this.moded = new BooleanWritable(false);
        this.edges = new EdgeWritables();
    }

    public NodeWritable(IntWritable id, IntWritable distance, boolean moded, EdgeWritables edges) {
        this.id = id;
        this.distance = distance;
        this.moded = new BooleanWritable(moded);
        this.edges = edges;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        distance.write(out);
        if (distance.get() >= 0){
            id.write(out);
            moded.write(out);
            edges.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        distance.readFields(in);
        if (distance.get() >= 0){
            id.readFields(in);
            moded.readFields(in);
            edges.readFields(in);
        }
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
