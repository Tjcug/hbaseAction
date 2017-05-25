package com.basic.hbase.util;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * locate com.basic.hbase.util
 * Created by 79875 on 2017/5/24.
 */
public class HBaseUtilsTest {

    private Logger logger= LoggerFactory.getLogger(HBaseUtilsTest.class);

    @Test
    public void creatTable() throws Exception {
        List<Result> psn = HBaseUtils.getAllRecord("psn");
        for(Result res :psn){
            logger.info(res.toString());
        }
    }

    @Test
    public void listTables() throws IOException {
        HBaseUtils.listTables();
    }
}
