package com.xiyou.platform.UserRecommend;

import com.xiyou.platform.hdfs.DataDBClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于用户协同过滤算法推荐
 *
 * @author cc
 * @create 2017-08-04-17:16
 */

public class UserCFRecommendClient {
    public static void main(String[] args) throws IOException {
        boolean result = runJob();
        if (result == false){
            System.out.println("erro");
        }
    }
    public static boolean runJob() throws IOException {
        boolean result = false;
        Map<String,String> path = new HashMap<String, String>();
        String input_file = DataDBClass.HDFS+"/user/hadoop/platform/UserCF";
        boolean flag = DataDBClass.copyFile(DataDBClass.getSrcFile(),input_file + "/data");
        if (!flag){
            System.out.println("copy error");
            System.exit(-3);
        }
        path.put("input_step1", input_file+"/data" );
        path.put("output_step1", input_file + "/step1");
        path.put("input_step2", path.get("output_step1"));
        path.put("output_step2", input_file + "/step2");
        path.put("input_step3", path.get("output_step2"));
        path.put("output_step3", input_file + "/step3");
        path.put("input_step4_1", path.get("input_step1"));
        path.put("output_step4_1", input_file + "/step4_1");
        path.put("input_step4_2_1", path.get("output_step4_1"));
        path.put("input_step4_2_2", path.get("output_step3"));
        path.put("output_step4_2", input_file + "/step4_2");
        path.put("input_step5", path.get("output_step4_2"));
        path.put("output_step5", input_file + "/step5");
        try {
            result = UserCF_Step1.runJob(path);
            result = UserCF_Step2.runJob(path);
            result = UserCF_Step3.runJob(path);
            result = UserCF_Step4_1.runJob(path);
            result = UserCF_Step4_2.runJob(path);
            result = UserCF_Step5.runJob(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;

    }
}
