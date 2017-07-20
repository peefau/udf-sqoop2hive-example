package cn.inspur.com;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by pingfuli on 2017/7/17.
 */

public class SqoopTest {

    private static void callShell(String[] shellString) throws Exception{
        try {

            for (String aShellString : shellString) {
                System.out.println(aShellString);
            }

//            Process p = Runtime.getRuntime().exec(new String[]{"ping","www.baidu.com"});
            Process p = Runtime.getRuntime().exec(shellString);
            InputStream is = p.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is,"UTF-8"));
            String line = null;
            while((line=br.readLine()) != null)
            {
                System.out.println(line);
            }
            System.out.println("");
            int exitValue = p.waitFor();
            if (0 != exitValue) {
                System.out.println(p.toString()+" call shell failed. error code is :" + exitValue);
                InputStream errorStream = p.getErrorStream();
                BufferedReader buf = new BufferedReader(new InputStreamReader(errorStream,"UTF-8"));//缓冲
                String lines = null;
                while((lines =buf.readLine())!=null) {
                    System.out.println(lines);
                }
            }
            is.close();
            br.close();
            p.destroy();

        } catch(Exception e){
                e.printStackTrace();
        }
    }
    //动态生成 sqoop command
    private static String[] dealSqoopCommand(JSONObject jsonObject){

        ArrayList list = new ArrayList();

        if (jsonObject.containsKey("oracle")){
            JSONObject jsonObj = jsonObject.getJSONObject("oracle");
            String operation = jsonObj.getString("operation");
            String connect = jsonObj.getString("connect");
            String username = jsonObj.getString("username");
            String password = jsonObj.getString("password");
            String partitionValue = jsonObj.getString("partition-value");
            String hiveTable = jsonObj.getString("hive-table");
            String splitColumn = jsonObj.getString("split-by");
            String numMappers = jsonObj.getString("num-mappers");
            String query = jsonObj.getString("query");

            list.add("sqoop");
            list.add(jsonObj.getString("operation"));
            list.add("--connect");
            list.add(jsonObj.getString("connect"));
            list.add("--username");
            list.add(jsonObj.getString("username"));
            list.add("--password");
            list.add(jsonObj.getString("password"));
            list.add("--hive-import");
            list.add("--hive-overwrite");
            list.add("--hive-table");
            list.add(jsonObj.getString("hive-table"));
            list.add("--delete-target-dir");
            list.add("--target-dir");
            list.add(jsonObj.getString("hive-table"));
            list.add("--split-by");
            list.add(jsonObj.getString("split-by"));
            list.add("--num-mappers");
            list.add(jsonObj.getString("num-mappers"));
            list.add("--hive-partition-key");
            list.add("partition_dt");
            list.add("--hive-partition-value");
            list.add(jsonObj.getString("partition-value"));
            list.add("--query");
            list.add(jsonObj.getString("query"));
        }
        String[] cmdStr = (String[])list.toArray(new String[list.size()]);

        return cmdStr;
    }

    public static void main(String[] args) {
        try {
            InputStream is = SqoopTest.class.getResourceAsStream("/resources/SqoopConfig.json");
            if(is != null){ //判断文件是否存在
                //考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
                String lineTxt = "";
                String tmpline = "";
                while((tmpline = bufferedReader.readLine()) != null){
                    lineTxt = lineTxt + tmpline;
                }
                bufferedReader.close();
                JSONObject jsonObj = JSON.parseObject(lineTxt);
                //动态生成 sqoop command
                String[] cmdStr = dealSqoopCommand(jsonObj);
//                System.out.println(cmdStr);
                callShell(cmdStr);
            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }

    }
}
