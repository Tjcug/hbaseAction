package com.basic.hbase;

import com.basic.hbase.util.HbaseNewAPI;
import org.junit.Test;

import java.io.IOException;

/**
 * locate com.basic.hbase
 * Created by 79875 on 2017/5/25.
 */
public class HbaseNewAPITest {

    @Test
    public void testNewAPI() throws IOException {
        HbaseNewAPI.listTables();
    }
}
