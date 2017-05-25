package com.basic.hbase.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * locate com.basic.hbase.mapreduce
 * Created by 79875 on 2017/5/25.
 */
public class WCTableReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable>{
    public static final byte[] CF="cf".getBytes();
    public static final byte[] COUNT="count".getBytes();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i=0;
        for(IntWritable intWritable:values){
            i+=intWritable.get();
        }
        Put put=new Put(key.toString().getBytes());
        put.addColumn(CF,COUNT, (""+i).getBytes());
        context.write(null,put);
    }
}
