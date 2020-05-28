package gzzy_bigdata_root.spark.kafka.config;

import gzzy_bigdata_root.common.config.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-12-29 20:44
 */
public class KafkaConfig1 {

    private static final String DEFAULT_KAFKA_CONFIG_PATH = "kafka/kafka-server-config.properties";
    private static Properties kafkaPro;
    static {
        kafkaPro = ConfigUtil.getInstance().getProperties(DEFAULT_KAFKA_CONFIG_PATH);
    }


    public static Map<String,String> getKafkaConfig(String groupId){
        Map map = new HashMap<String,String>();
        map.put("bootstrap.servers",kafkaPro.get("bootstrap.servers"));
        map.put("group.id",groupId);
        map.put("auto.offset.reset","earliest");
        map.put("enable.auto.commit","false");//禁止自动提交offset
        map.put("auto.create.topics.enable","true");
        map.put("refresh.leader.backoff.ms","1000");
        map.put("num.consumer.fetchers","8");
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return map;
    }

    public static void main(String[] args) {
        System.out.println(KafkaConfig.getKafkaConfig("aaa").toString());
    }

}
