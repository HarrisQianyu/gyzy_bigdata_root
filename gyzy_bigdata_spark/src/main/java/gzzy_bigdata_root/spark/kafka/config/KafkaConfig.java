package gzzy_bigdata_root.spark.kafka.config;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-06 22:13
 */
public class KafkaConfig {


    /*  "bootstrap.servers" -> "had-11:9092,had-12:9092,had-13:9092,had-14:9092,had-15:9092",
              "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean)*/

    public static Map<String,Object> getKafkaConfig(String groupId){
        Map map = new HashMap<String,String>();
        map.put("bootstrap.servers","cdh01:9092,cdh02:9092,cdh03:9092");
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("group.id",groupId);
        map.put("auto.offset.reset","earliest");
        map.put("enable.auto.commit",false);
        return map;
    }
}
