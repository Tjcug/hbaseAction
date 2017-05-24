package com.basic.hbase;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * locate com.basic.hbase
 * Created by 79875 on 2017/5/24.
 */
public class HbaseTest {
    private HBaseAdmin hBaseAdmin;
    private HTable hTable;

    public static final String TN="phone";

    @Before
    public void begin() throws IOException, ConfigurationException {
        Configuration conf=new Configuration();
        org.apache.commons.configuration.Configuration config = new PropertiesConfiguration("hbase.properties");
        String hbase_zookeeper_client_port = config.getString("hbase.zk.port");
        String hbase_zookeeper_quorum = config.getString("hbase.zk.host");
        String hbase_master = config.getString("hbase.master");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_client_port);
        conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
        conf.set("hbase.master", hbase_master);
        hBaseAdmin=new HBaseAdmin(conf);

        hTable=new HTable(conf,TN);
    }

    @After
    public void end() throws IOException {
        if(hBaseAdmin!=null){
            hBaseAdmin.close();
        }
        if(hTable!=null)
            hTable.close();
    }

    @Test
    public void createTable() throws IOException {
        if(hBaseAdmin.tableExists(TN)){
            hBaseAdmin.disableTable(TN);
            hBaseAdmin.deleteTable(TN);
        }

        HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf(TN));
        HColumnDescriptor family = new HColumnDescriptor("cf1");

        family.setBlockCacheEnabled(true);
        family.setInMemory(true);
        family.setMaxVersions(1);

        hTableDescriptor.addFamily(family);

        hBaseAdmin.createTable(hTableDescriptor);
    }

    @Test
    public void insert() throws IOException {
        String rowkey="0001";
        Put put=new Put(rowkey.getBytes());
        hTable.put(put);
    }
}
