package es;

import hadoop.HadoopClient;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class QueryAll {

    private static final Log log = LogFactory.getLog(QueryAll.class);
    public static int count=1;
    public static void main(String[] args) throws IOException {
        QueryAll queryAll = new QueryAll();
        String index=args[0];
       // String index="apk_method_0";
        queryAll.querydata(index);
    }



    public  TransportClient getClient(){

        Settings settings = Settings.builder()
                .put("client.transport.sniff", true)
                .put("cluster.name", "CertCluster").build();

        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("172.31.20.161"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("172.31.20.162"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("172.31.20.163"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return client;
    }

    public void querydata (String index) throws IOException {

       // HashMap<String,String> map=new HashMap<>();
        TransportClient client = getClient();

        SearchRequestBuilder requestBuilder = client.prepareSearch(index);

        requestBuilder.setSize(100000);
        requestBuilder.setFetchSource("value", "");
        requestBuilder.setScroll(new TimeValue(200000));
        requestBuilder.setTypes("sdata");


        SearchResponse response = requestBuilder.execute().actionGet();
        String scrollId = "";



        saveHits(response.getHits(),index);

        while (response.getHits().getHits().length>0) {

            try {
                scrollId = response.getScrollId();

                response = client.prepareSearchScroll(scrollId)
                        .setScroll(TimeValue.timeValueMinutes(2))//设置查询context的存活时间
                        .execute()
                        .actionGet();

                saveHits(response.getHits(),index);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("报错继续");
                try {
                    Thread.sleep(1000*30);



                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }


            }

        }



    }





    public void saveHits(SearchHits hits,String index) throws IOException {
        ArrayList<String> list = new ArrayList<>();
        ArrayList<String> errlist = new ArrayList<>();
        int i=0;
        for(SearchHit h:hits){

            String sha1 = null;
            String value = null;

            try {
                sha1 = h.field("_parent").getValue().toString();

                value = h.getSourceAsMap().get("value").toString();
                if(value.contains("\n")){
                    value=value.replace("\n","\\n" );
                }

            } catch (Exception e) {
                log.error(index+"   报错 skip "+sha1 +"  id=   "+h.getId());
                errlist.add(h.getId());
                continue;
            }

            String line=sha1+"||"+value;
            list.add(line);
           // System.out.println(line);
        }
       // log.info("-----"+count++);
        HadoopClient.saveHadoop(list,errlist, index);
    }


}
