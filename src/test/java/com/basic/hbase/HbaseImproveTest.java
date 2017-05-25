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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * locate com.basic.hbase
 * 利用Google PortocolBuffer 优化HBase
 * 减少数据存储占用大小,优化Hbase存储空间
 * Created by 79875 on 2017/5/25.
 */
public class HbaseImproveTest {
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
     * @param prefix 年月日
     * @return
     */
    public String getDate2(String prefix){
        return prefix+String.format("%02d%02d%02d",new Object[]{random.nextInt(24),random.nextInt(60),random.nextInt(60)});
    }

    /**
     * 利用Google PortocolBuffer 优化HBase
     * 减少数据存储占用大小
     * 十个手机号 一天内随机产生100条通话记录
     * @throws ParseException
     * @throws IOException
     */
    @Test
    public void insertDB2() throws ParseException, IOException {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");

        for(int i=0;i<10;i++){
            String rowkey = "";
            String phonenumber=getPhoneNumber("186");

            //一天的通话记录
            Phone.phoneday.Builder pday=Phone.phoneday.newBuilder();
            for(int j=0;j<100;j++){
                String phoneDate=getDate2("20170525");
                long dateTimeLong = sdf.parse(phoneDate).getTime();
                rowkey= phonenumber+"_"+String.valueOf(Long.MAX_VALUE - dateTimeLong);
                log.info(rowkey);

                //一条通话记录
                Phone.phonedetail.Builder detail = Phone.phonedetail.newBuilder();
                detail.setDuration(random.nextInt(1000)+"");
                detail.setPnumber(getPhoneNumber("130"));
                detail.setTime(phoneDate);
                detail.setType(random.nextInt(2)+"");

                pday.addPhonelist(detail);

            }
            Put put=new Put(rowkey.getBytes());
            put.addColumn("cf1".getBytes(),"pday".getBytes(),pday.build().toByteArray());

            hTable.put(put);
        }
    }

    /**
     * 18695803779_9223370541180778807
     * 得到这个手机号码这一天的所有通话记录
     * rowkey 18695803779_9223370541180778807
     */
    @Test
    public void getPhoneData() throws IOException {
        Get get=new Get("18695803779_9223370541149901807".getBytes());
//        get.addColumn("cf1".getBytes(),"pday".getBytes());
        Result result = hTable.get(get);
        Cell cell = result.getColumnLatestCell("cf1".getBytes(), "pday".getBytes());
        Phone.phoneday phoneday = Phone.phoneday.parseFrom(CellUtil.cloneValue(cell));
        for(Phone.phonedetail phonedetail: phoneday.getPhonelistList()){
            log.info(phonedetail.toString());
        }
    }

}
