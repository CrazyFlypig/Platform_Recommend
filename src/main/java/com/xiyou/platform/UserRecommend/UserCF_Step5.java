package com.xiyou.platform.UserRecommend;

import com.xiyou.platform.hdfs.DataDBClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * 排序推荐结果
 *
 * @author cc
 * @create 2017-08-08-2:30
 */

public class UserCF_Step5 {
    public static class UserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map (LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String[] in = value.toString().split("\t");
            context.write(new IntWritable(Integer.parseInt(in[0])),new Text(in[1]));
        }
    }
    public static class UserReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> map = new HashMap<String, Double>();
            List<Double> list = new LinkedList<Double>();
            List<Item> list1 = new LinkedList<Item>();
            for ( Text vs : values){
                String[] value = vs.toString().split(" ");
                for (String in : value
                        ) {
                    String item = in.substring(0,in.indexOf("["));
                    String platform = in.substring(in.indexOf("[")+1,in.indexOf("]"));
//                    map.put(item,Double.parseDouble(platform));
//                    list.add(Double.parseDouble(platform));
                    list1.add(new Item(item,Double.parseDouble(platform)));
                }
            }
//            Collections.sort(list, new Comparator<Double>() {
//                public int compare(Double o1, Double o2) {
//                    return o2.compareTo(o1);
//                }
//            });
//            String[] item = new String[list.size()];
//            Iterator<String> it = map.keySet().iterator();
            String v = "";
//            while (it.hasNext()){
//                String s = it.next();
//                item[list.indexOf(map.get(s))] = s;
//            }
            for (Item item : list1){
                String s = item.getItem();
                Double platform = item.getPlatform();
                v += s + "[" + platform +"]" +" ";
            }
            context.write(key,new Text(v));
        }
    }
    public static boolean runJob (Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = DataDBClass.getConf();

        String input = path.get("input_step5");
        String output =path.get("output_step5");

        System.out.println(DataDBClass.exists(input));
        if ( !DataDBClass.exists(input) ){
            System.out.println(input);
            System.out.println("源文件不存在");
            System.exit(-5);
        }
        if ( DataDBClass.exists(output) ){
            DataDBClass.rmrDir(output);
        }

        Job job = Job.getInstance(conf, "UserCF_Step5 job");
        job.setJarByClass(UserCF_Step5.class);
        job.setMapperClass(UserCF_Step5.UserMapper.class);
        job.setReducerClass(UserCF_Step5.UserReduce.class);
        job.setOutputKeyClass(IntWritable.class);
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
