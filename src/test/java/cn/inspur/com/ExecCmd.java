package cn.inspur.com;

/**
 * Created by pingfuli on 2017/7/18.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ExecCmd {
    public static Logger log = LoggerFactory.getLogger(ExecCmd.class);

    public static void main(String args[]) {
        Process process;
        List<String> processList = new ArrayList<String>();
        try {
            process = Runtime.getRuntime().exec("ps -aux");
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                processList.add(line);
            }
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String line : processList) {
            System.out.println(line);
        }
    }


    public static void callShell(String shellString) {
        try {
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                log.error("call shell failed. error code is :" + exitValue);
            }
        } catch (Throwable e) {
            log.error("call shell failed. " + e);
        }
    }
}

