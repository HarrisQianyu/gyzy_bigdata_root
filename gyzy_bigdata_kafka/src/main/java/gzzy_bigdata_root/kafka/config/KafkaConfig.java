package gzzy_bigdata_root.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import gzzy_bigdata_root.common.config.ConfigUtil;

import java.util.Properties;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-01 20:31
 */
public class KafkaConfig {
    private static final Logger LOG = Logger.getLogger(KafkaConfig.class);

    //kafka配置路径
    private static final String DEFAULT_KAFKA_CONFIG_PATH = "kafka/kafka-server-config.properties";

    private Properties kafkaProperties;
    private ProducerConfig ProducerConfig;

    private static volatile KafkaConfig kafkaConfig = null;

    //私有构造方法
    private KafkaConfig(){
        LOG.info("开始读取kafka配置文件");
        kafkaProperties = ConfigUtil.getInstance().getProperties(DEFAULT_KAFKA_CONFIG_PATH);
        ProducerConfig = new ProducerConfig(kafkaProperties);
        LOG.info("实例化ProducerConfig成功");
    }

    //提供一个对外公用方法
    public static KafkaConfig getInstance(){
        //双重否定
        if(kafkaConfig == null){
            //构造
            synchronized (ConfigUtil.class){
                if(kafkaConfig == null){
                    kafkaConfig = new KafkaConfig();
                }
            }
        }
        return kafkaConfig;
    }

    /**
     * 获取kafka   properties对象
     * @return
     */
    public Properties getKafkaProperties(){
        return kafkaProperties;
    }


}
