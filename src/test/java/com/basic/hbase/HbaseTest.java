package com.basic.hbase;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * locate com.basic.hbase
 * Created by 79875 on 2017/5/24.
 */
public class HbaseTest {
    private HBaseAdmin hBaseAdmin;
    private HTable hTable;

    public static final String TN="phone";
    private static Logger log= LoggerFactory.getLogger(HbaseTest.class);

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

    private Random random=new Random();

    /**
     * 随机生成电话号码
     * @param prefix 电话号码前缀130、186、170
     * @return
     */
    public String getPhoneNumber(String prefix){
        return prefix+String.format("%08d", random.nextInt(99999999));
    }

    /**
     * 随机生成时间
     * @param year 年份2017
     * @return
     */
    public String getDate(String year){
        return year+String.format("%02d%02d%02d%02d%02d",new Object[]{random.nextInt(12)+1,random.nextInt(30)+1,random.nextInt(24),random.nextInt(60),random.nextInt(60)});
    }

    /**
     * 插入十个手机号 100条通话记录
     * 满足查询 时间降序排序
     */
    @Test
    public void insertDB() throws ParseException, IOException {
        List<Put> putList=new ArrayList<>();
        for(int i=0;i<10;i++){
            String rowkey;
            String phonenumber=getPhoneNumber("186");
            for(int j=0;j<100;j++){
                String phoneDate=getDate("2017");
                SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
                long dateTimeLong = sdf.parse(phoneDate).getTime();
                rowkey= phonenumber+"_"+String.valueOf(Long.MAX_VALUE - dateTimeLong);

                log.info(rowkey);

                Put put=new Put(rowkey.getBytes());
                put.addColumn("cf1".getBytes(),"type".getBytes(),(random.nextInt(2)+"").getBytes());
                put.addColumn("cf1".getBytes(),"time".getBytes(),phoneDate.getBytes());//设置时间戳
                put.addColumn("cf1".getBytes(),"duration".getBytes(),(random.nextInt(1000)+"").getBytes());//设置通话时长
                put.addColumn("cf1".getBytes(),"pnumber".getBytes(),getPhoneNumber("130").getBytes());
                putList.add(put);
            }
        }
        hTable.put(putList);
    }

    /**
     * 查询某个手机号下某个月份的所有通话详单
     */
    @Test
    public void scanDB() throws IOException, ParseException {
        //18692504352_2016 二月份
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

    /**
     * 使用Fliter过滤器 进行Scan Hbase
     * 查询某个手机号 所有主叫type=0 的通话详单
     */
    @Test
    public void scanDB2() throws IOException {
        //18692504352
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //前缀过滤器 针对rowkey
        PrefixFilter prefixFilter=new PrefixFilter("18692504352".getBytes());
        SingleColumnValueFilter valueFilter=new SingleColumnValueFilter("cf1".getBytes(),"type".getBytes(), CompareFilter.CompareOp.EQUAL,"1".getBytes());
        filterList.addFilter(prefixFilter);
        filterList.addFilter(valueFilter);
        Scan scan=new Scan();

        scan.setFilter(filterList);
        ResultScanner scanner = hTable.getScanner(scan);
        for(Result res : scanner){
            log.info("===================================================");
            String rowkey=new String(res.getColumnLatestCell("cf1".getBytes(),"type".getBytes()).getRow());
            log.info("rowKey: "+rowkey);
            for(Cell cell : res.rawCells()){
                log.info("列修饰符为："+new String(CellUtil.cloneQualifier(cell))+" 值为：" + new String(CellUtil.cloneValue(cell)));
            }
            log.info("===================================================");
        }
    }
}
