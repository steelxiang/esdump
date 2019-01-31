package hbase;

import es.QueryAll;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class HbaseUtil {

    private static final Log log = LogFactory.getLog(HbaseUtil.class);


    public static void putdata(String type, HashMap<String, ArrayList<String>> hbaselist) throws IOException {
        String name = null;
        String cf = null;
        if ("strings".equals(type)) {
            name = "APK_STRING";
            cf = "string";
        }
        if ("field".equals(type)) {
            name = "APK_FIELD";
            cf = "filed";
        }
        if ("class".equals(type)) {
            name = "APK_CLASS";
            cf = "class";
        }
        if ("method".equals(type)) {
            name = "APK_METHOD";
            cf = "method";
        }

        TableName tableName = TableName.valueOf(name);
        long num = 0;
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "cdh176,cdh157,cdh177");
        //  config.set("hbase.zookeeper.quorum", "172.31.102.203,172.31.102.204,172.31.102.205");
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        Table table = connection.getTable(tableName);
        ArrayList<Put> puts = new ArrayList<>();

        Set<String> strings = hbaselist.keySet();

        for (String sha : strings) {

            ArrayList<String> arrayList = hbaselist.get(sha);

            ArrayList<String> ranlist = new ArrayList<>();
            if (arrayList.size() < 1) {
                continue;
            }
            for (String value : arrayList) {

                String str = RandomStringUtils.random(8, "abcdefghijklmnopqrstuvwxyz1234567890");

                while (ranlist.contains(str)) {
                    log.info("随机码重复，重新生成");
                    str = RandomStringUtils.random(8, "abcdefghijklmnopqrstuvwxyz1234567890");
                    ranlist.add(str);
                }


                String rowkey = str + sha;

                Put put = new Put(rowkey.getBytes());
                put.addColumn(cf.getBytes(), ("apk_" + cf).getBytes(), value.getBytes());

                puts.add(put);

            }

        }

        log.info("puts 数量：--" + puts.size() + "-------");

        table.put(puts);
        table.close();
        connection.close();


    }



}
