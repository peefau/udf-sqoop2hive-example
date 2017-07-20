package cn.inspur.com;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pingfuli on 2017/7/6.
 */
public class Hive2RedisUDF extends UDF{
    public static Map<String,String> testMap = new HashMap<String,String>();

    static
    {
        testMap.put("china","中国");
        testMap.put("america","美国");
    }

    Text t = new Text();

    public Text evaluate(Text nation){
        String nationE = nation.toString();
        String name = testMap.get(nationE);
        if(name == null) {
            name = "火星人";
        }
        t.set(name);
        return t;
    }
}
