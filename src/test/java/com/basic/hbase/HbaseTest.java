package com.basic.hbase;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * locate com.basic.hbase
 * Created by 79875 on 2017/5/24.
 */
public class HbaseTest {
    private HBaseAdmin hBaseAdmin;
    private HTable hTable;

    public static final String TN="phone";
    private Logger log= LoggerFactory.getLogger(HbaseTest.class);

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
        //RowKey的设计
        //手机号_时间戳
        String rowkey="13072783289_2016123123123";
        Put put=new Put(rowkey.getBytes());

        put.addColumn("cf1".getBytes(), "type".getBytes(),"1".getBytes());
        //打电话的时间 通话时长
        put.addColumn("cf1".getBytes(), "time".getBytes(),"100".getBytes());
        //目标手机号码
        put.addColumn("cf1".getBytes(), "pnumber".getBytes(),"177123123123".getBytes());

        hTable.put(put);
    }

    @Test
    public void get() throws IOException {
        String rowkey="13072783289_2016123123123";
        Get get=new Get(rowkey.getBytes());
//        get.addColumn("cf1".getBytes(),"type".getBytes());
//        get.addColumn("cf1".getBytes(),"time".getBytes());

        Result res = hTable.get(get);
        Cell celltype = res.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
        log.info(new String(CellUtil.cloneValue(celltype)));
        Cell celltime = res.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
        log.info(new String(CellUtil.cloneValue(celltime)));
//        for(Cell cell : res.rawCells()){
//            log.debug("列簇为：" + new String(CellUtil.cloneFamily(cell)));
//            log.debug("列修饰符为："+new String(CellUtil.cloneQualifier(cell)));
//            log.debug("值为：" + new String(CellUtil.cloneValue(cell)));
//        }
    }
}
