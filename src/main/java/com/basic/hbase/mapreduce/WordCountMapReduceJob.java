package com.basic.hbase.mapreduce;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * locate com.basic.hbase.mapreduce
 * Created by 79875 on 2017/5/25.
 * hadoop jar hbaseAction-1.0-SNAPSHOT.jar com.basic.hbase.mapreduce.WordCountMapReduceJob /user/root/wordcount/input
 */
public class WordCountMapReduceJob {
    public static void main(String[] args) throws Exception {
        org.apache.commons.configuration.Configuration config = new PropertiesConfiguration("hbase.properties");
        String hbase_zookeeper_quorum = config.getString("hbase.zk.host");

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);

        Job job = Job.getInstance(conf, "hbase word count");
        job.setJarByClass(WordCountMapReduceJob.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //hbase reducer
        String tableName="wordcount";
        TableMapReduceUtil.initTableReducerJob(tableName,WCTableReducer.class,job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
