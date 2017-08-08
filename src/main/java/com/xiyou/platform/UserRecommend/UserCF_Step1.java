package com.xiyou.platform.UserRecommend;

import com.xiyou.platform.hdfs.DataDBClass;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;


/**
 * 基于用户的协同过滤算法第一步
 *
 * @author cc
 * @create 2017-08-05-11:13
 */

public class UserCF_Step1 {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map (LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String[] values= value.toString().split(",");
            if ( values.length == 3 ){
                Text k = new Text( values[1] );
                Text v = new Text( values[0] + "," + values[2] );
                context.write(k, v);
            }
        }
    }
    public static class UserReduce extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> map = new HashMap<String, String>();
            List<String> list = new LinkedList<String>();
            for (Text value : values){
                String[] strs = value.toString().split(",");
                if ( strs.length >= 2 ){
                    map.put(strs[0], strs[1]);
                    list.add(strs[0]);
                }
            }
            for (int i=0; i<list.size(); i++){
                String k1 = list.get(i);
                String v1 = map.get(k1);
                for (int j=0;j<list.size();j++){
                    if (j == i ){
                        continue;
                    }
                    String k2 = list.get(j);
                    String v2 = map.get(k2);
                    context.write(new Text(k1 + "," + k2),new Text(v1 + "," + v2));
                }
            }
        }
    }
    public static boolean runJob (Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = DataDBClass.getConf();

        String input = path.get("input_step1");
        String output =path.get("output_step1");

        if ( !DataDBClass.exists(input) ){
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( DataDBClass.exists(output) ){
            DataDBClass.rmrDir(output);
        }

        Job job = Job.getInstance(conf, "UserCF_Step1 job");
        job.setJarByClass(UserCF_Step1.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        if (job.waitForCompletion(true)){
            return true;
        }else {
            return false;
        }
    }
}
