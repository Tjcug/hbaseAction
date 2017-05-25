package com.basic.hbase.main;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * locate com.basic.hbase.main
 * Created by 79875 on 2017/5/25.
 * java -cp hbaseAction-1.0-SNAPSHOT.jar com.basic.hbase.main.HbaseScanMain
 */
public class HbaseScanMain {
    private static HTable hTable;
    private static Admin admin;
    private static Connection connection;

    public static final String TN="phone";
    private static Logger log= LoggerFactory.getLogger(HbaseScanMain.class);

    public static void initConnection() throws IOException, ConfigurationException {
        Configuration conf=new Configuration();
        org.apache.commons.configuration.Configuration config = new PropertiesConfiguration("hbase.properties");
        String hbase_zookeeper_client_port = config.getString("hbase.zk.port");
        String hbase_zookeeper_quorum = config.getString("hbase.zk.host");
        String hbase_master = config.getString("hbase.master");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_client_port);
        conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
        conf.set("hbase.master", hbase_master);
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();

        hTable = (HTable) connection.getTable(TableName.valueOf(TN));
    }

    public static void main(String[] args) throws ParseException, IOException, ConfigurationException {
        initConnection();
        Scan scan=new Scan();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
        String startRowKey="18692504352_"+(Long.MAX_VALUE-sdf.parse("20170901000000").getTime());
        scan.setStartRow(startRowKey.getBytes());
        String stopRowKey="18692504352_"+(Long.MAX_VALUE-sdf.parse("20170801000000").getTime());
        scan.setStopRow(stopRowKey.getBytes());

        ResultScanner scanner = hTable.getScanner(scan);
        for(Result res : scanner){
            log.info("===================================================");
            for(Cell cell : res.rawCells()){
                log.info("列修饰符为："+new String(CellUtil.cloneQualifier(cell))+" 值为：" + new String(CellUtil.cloneValue(cell)));
            }
            log.info("===================================================");
        }
    }
}
