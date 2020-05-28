package gzzy_bigdata_root.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import gzzy_bigdata_root.kafka.config.KafkaConfig;
/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-01 19:59
 */
public class StringProducer {
    private static final Logger LOG = Logger.getLogger(StringProducer.class);


    //定义一个发送方法  topic,line

    /**
     *
     * @param topic 发送的kafka主题
     * @param line  发送的内容
     */
    public static void producer(String topic,String line){
        KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getInstance().getKafkaProperties());
        producer.send(new ProducerRecord<>(topic,line));
        LOG.info("向kafka主题"+topic+"发送数据" + line);
        producer.close();
    }


    public static void main(String[] args) {
        StringProducer.producer("test1","1111111111111");
    }


}
