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
 * 输入数据准备
 *
 * @author cc
 * @create 2017-08-07-22:37
 */

public class UserCF_Step4_1 {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String[] values= value.toString().split(",");
            if ( values.length == 3 ){
                Text k = new Text( values[0] );
                Text v = new Text( values[1] + "," + values[2] );
                context.write(k, v);
            }
        }
    }
    //userID itemID,score
    public static class UserReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> itemscore = new HashMap<String, Double>();
            for (Text value : values ) {
                String[] vs = value.toString().split(",");
                Double score = Double.parseDouble(vs[1].toString());
                itemscore.put(vs[0].toString(), score);
            }
            Iterator<String> it = itemscore.keySet().iterator();
            String v = "";
            while ( it.hasNext() ){
                String item = it.next();
                Double score = itemscore.get(item);
                if (score > 3.0){
                    v += item + "," +score +" ";
                }
            }
            context.write(key,new Text(v));
        }
    }
    public static boolean runJob (Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = DataDBClass.getConf();

        String input = path.get("input_step4_1");
        String output =path.get("output_step4_1");

        if ( !DataDBClass.exists(input) ){
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( DataDBClass.exists(output) ){
            DataDBClass.rmrDir(output);
        }

        Job job = Job.getInstance(conf, "UserCF_Step1 job");
        job.setJarByClass(UserCF_Step4_1.class);
        job.setMapperClass(UserCF_Step4_1.UserMapper.class);
        job.setReducerClass(UserCF_Step4_1.UserReduce.class);
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
