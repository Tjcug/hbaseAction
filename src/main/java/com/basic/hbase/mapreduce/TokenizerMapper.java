package com.basic.hbase.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * locate com.basic.hbase.mapreduce
 * Created by 79875 on 2017/5/25.
 */
public class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable> {
    private Logger logger= LoggerFactory.getLogger(TokenizerMapper.class);
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
