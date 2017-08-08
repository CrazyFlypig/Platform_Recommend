package com.xiyou.platform.UserRecommend;

import com.xiyou.platform.hdfs.DataDBClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 基于用户的协同过滤算法第二步
 * @author cc
 * @create 2017-08-05-16:16
 */

public class UserCF_Step2 {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            context.write(new Text(values[0]),new Text(values[1]));
        }
    }
    public static class UserReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double similarity = 0.0;
            int num = 0;
            for (Text value : values){
                String[] vs = value.toString().split(",");
                if ( vs.length >= 2){
                    sum += Math.pow(Double.parseDouble(vs[0]) - Double.parseDouble(vs[1]), 2);
                    num += 1;
                }
            }
            if (sum != -1 ){
                similarity = (double)num/(num+Math.sqrt(sum));//欧式距离
            }
            context.write(key, new Text(String.format("%.7f",similarity)));
        }
    }
    public static boolean runJob (Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = DataDBClass.getConf();

        String input = path.get("input_step2");
        String output =path.get("output_step2");

        System.out.println(DataDBClass.exists(input));
        if ( !DataDBClass.exists(input) ){
            System.out.println(input);
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( DataDBClass.exists(output) ){
            DataDBClass.rmrDir(output);
        }

        Job job = Job.getInstance(conf, "UserCF_Step2 job");
        job.setJarByClass(UserCF_Step2.class);
        job.setMapperClass(UserCF_Step2.UserMapper.class);
        job.setReducerClass(UserCF_Step2.UserReduce.class);
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
