package hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;

public class HadoopClient {
   // public static FileSystem fs;
   // public static Configuration conf = new Configuration();
    private static final Log log = LogFactory.getLog(HadoopClient.class);
    private static int count=1;

   public static void saveHadoop(ArrayList<String> list ,ArrayList<String> errlist,String path) throws IOException {

       Configuration conf = new Configuration();
       conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020");
       conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
       System.setProperty("HADOOP_USER_NAME", "misas_dev");
       FileSystem fs = FileSystem.get(conf);
       String fspath="/user/misas_dev/data/es/apk_method/";
        String errpath="/user/misas_dev/data/es/apk_method_err/";


       Path p=new Path(fspath+path+".txt");
       Path errp=new Path(errpath+path+".txt");

      if(!fs.exists(p)){
          fs.create(p).close();
      }
       if(!fs.exists(errp)){
           fs.create(errp).close();
       }

       FSDataOutputStream append = fs.append(p);

       for(String s:list){

           append.writeBytes(s+"\n");
       }
      log.info(path+"  append  finish---"+count++);
       append.close();

       FSDataOutputStream errap = fs.append(errp);

       for(String s:errlist){

           errap.writeBytes(s+"\n");
       }
       errap.close();

      fs.close();
   }
}
