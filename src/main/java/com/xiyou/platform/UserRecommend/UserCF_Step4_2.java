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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;


/**
 * 基于用户的协同过滤算法，第四步
 *
 * @author cc
 * @create 2017-08-07-15:09
 */

public class UserCF_Step4_2 {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String DataName ;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit)context.getInputSplit();
            DataName = split.getPath().getParent().getName();
        }
        @Override
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            if (DataName.equals("step3")){
                String[] user = values[1].split(",");
                if (user.length >= 2){
//                    System.out.println("Map_step3: key-" + user[0]+ " value-" +"UserID," + values[0]+","+user[1]);
                    context.write(new Text(user[0]),new Text("UserID," + values[0]+","+user[1]));
                }
            }
            if (DataName.equals("step4_1")){
//                System.out.println("Map_step4: key-" + values[0]+ " value-" + values[1]);
                context.write(new Text(values[0]),new Text("ItemID,"+values[1]));
            }
        }
    }
    public static class UserReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> usermap = new HashMap<String, Double>();
            Map<String,Double> item = new HashMap<String, Double>();
            for (Text it : values ){
              String value = it.toString();
              if (value.startsWith("ItemID")) {
                  String[] vs = value.split(" ");
                  for ( String v: vs
                       ) {
                      String[] ss = v.split(",");
                      if (ss.length >= 2){
                          if (ss[0].equals("ItemID")){
                              item.put(ss[1],Double.parseDouble(ss[2]));
                          }else{
                              item.put(ss[0],Double.parseDouble(ss[1]));
                          }
                      }
                  }
              }
              if (value.startsWith("UserID")){
                  String[] vs = value.split(",");
                  if (vs.length >= 3){
                      usermap.put(vs[1],Double.parseDouble(vs[2]));
                  }
              }
            }
//            Iterator<String> it = item.keySet().iterator();
//            while (it.hasNext()){
//                String s = it.next();
//                Double d = item.get(s);
//                System.out.println("s :" + s + " d : "+ d );
//            }

            Iterator<String> user = usermap.keySet().iterator();
            while (user.hasNext()){
                String userID = user.next();
                Double similarity = usermap.get(userID);
                String platform = "";
                Iterator<String>  it = item.keySet().iterator();
                while (it.hasNext()){
                    String us = it.next();
                    Double score = item.get(us);
                    Double Platform = similarity + score;
                    platform += us + "[" + Platform + "]" + " ";
                }
                context.write(new Text(userID),new Text(platform));
             }

         }
    }
    public static boolean runJob (Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = DataDBClass.getConf();

        String input1 = path.get("input_step4_2_1");
        String input2 = path.get("input_step4_2_2");
        String output =path.get("output_step4_2");

        System.out.println(DataDBClass.exists(input1));
        if ( !DataDBClass.exists(input1) ){
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( !DataDBClass.exists(input2) ){
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( DataDBClass.exists(output) ){
            DataDBClass.rmrDir(output);
        }

        Job job = Job.getInstance(conf, "UserCF_Step4_2 job");
        job.setJarByClass(UserCF_Step4_2.class);
        job.setMapperClass(UserCF_Step4_2.UserMapper.class);
        job.setReducerClass(UserCF_Step4_2.UserReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(input2));
        FileInputFormat.addInputPath(job,new Path(input1));
        FileOutputFormat.setOutputPath(job,new Path(output));


        if (job.waitForCompletion(true)){
            return true;
        }else {
            return false;
        }
    }
}
