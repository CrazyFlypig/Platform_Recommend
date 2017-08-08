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
import java.util.*;

/**
 * 基于用户的协同过滤算法第三步
 * @author cc
 * @create 2017-08-05-16:16
 */

public class UserCF_Step3 {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String[] values1 = value.toString().split("\t");
            String[] values2 = values1[0].toString().split(",");
            context.write(new Text(values2[0]),new Text(values2[1]+ "," + values1[1]));
        }
    }
    public static class UserReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Double,String> map = new TreeMap<Double, String>( new Comparator<Double>() {
                public int compare(Double o1, Double o2) {
                    return o2.compareTo(o1);
                }
            });
            for (Text vs : values ){
                String[] v = vs.toString().split(",");
                map.put(Double.parseDouble(v[1]),v[0]);
            }
            Iterator<Double> it = map.keySet().iterator();
            int i = 0;
            String v = "";
            while (it.hasNext() && i<1 ){
                Double similarity = it.next();
                String userID = map.get(similarity);
                v += "," + userID + "," + String.format("%.7f",similarity);
                i++;
            }
            context.write(key,new Text(v.substring(1)));
        }
    }
    public static boolean runJob (Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = DataDBClass.getConf();

        String input = path.get("input_step3");
        String output =path.get("output_step3");

        System.out.println(DataDBClass.exists(input));
        if ( !DataDBClass.exists(input) ){
            System.out.println(input);
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( DataDBClass.exists(output) ){
            DataDBClass.rmrDir(output);
        }

        Job job = Job.getInstance(conf, "UserCF_Step3 job");
        job.setJarByClass(UserCF_Step3.class);
        job.setMapperClass(UserCF_Step3.UserMapper.class);
        job.setReducerClass(UserCF_Step3.UserReduce.class);
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
