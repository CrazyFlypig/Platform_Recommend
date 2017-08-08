package com.xiyou.platform.ItemRecommend;

import com.xiyou.platform.hdfs.DataDBClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * 基于物品协同过滤算法推荐
 *
 * @author cc
 * @create 2017-08-04-17:14
 */

public class ItemCFRecommendClient {
    public static void main(String[] args) throws IOException {
       Updata();
    }
    public static boolean runJob(){
        boolean result = false;
        return result;
    }
    public static void Updata() throws IOException {
        String local_file = "D:\\Test\\hadoop\\data";
        String src_file = DataDBClass.HDFS + "/user/hadoop/platform/";
        File local = new File(local_file);
        Path src = new Path(src_file);
        FileSystem fs = FileSystem.get(DataDBClass.getConf());
        if (!local.exists()){
            System.out.println("找不到本地文件");
            System.exit(-1);
        }
        if ( !fs.exists(src) ){
            fs.mkdirs(src);
        }
        fs.copyFromLocalFile(false,new Path(local_file),src);
        fs.close();
    }
}
