package com.xiyou.platform.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * hdfs数据存储
 *
 * @author cc
 * @create 2017-08-04-17:22
 */

public class DataDBClass {
    private static Configuration conf = new Configuration(true);
    private static FileSystem fs = null;
    public static String HDFS = "hdfs://192.168.1.154:9000";
    static {
        //指定hadoop fs的地址
        conf.set("fs.default.name", HDFS);
    }
    private static final String srcFile = HDFS + "/user/hadoop/platform/data";
    private static final String desFile = "";
    public static String getSrcFile(){
        return srcFile;
    }
    public static String getDesFile(){
        return desFile;
    }
    public static Configuration getConf(){
        return conf;
    }
    public static boolean rmrDir (String str) throws IOException {
        boolean result = false;
        fs = FileSystem.get(conf);
        Path path = new Path(str);
        if ( fs.exists(path) ){
            result = fs.delete(path,true);
        }
        fs.close();
        return result;
    }
    public static boolean copyFile (String srcs, String  dest) throws IOException {
        fs = FileSystem.get(conf);
        Path srcsPath = new Path(srcs);
        Path destPath = new Path(dest);
        boolean result = FileUtil.copy(FileSystem.get(srcsPath.toUri(),conf),srcsPath,FileSystem.get(destPath.toUri(),conf),destPath,false,true,conf);
        fs.close();
        return result;
    }
    public static boolean exists (String path) throws IOException {
        fs = FileSystem.get(conf);
        boolean result = fs.exists(new Path(path));
        fs.close();
        return result;
    }
}
