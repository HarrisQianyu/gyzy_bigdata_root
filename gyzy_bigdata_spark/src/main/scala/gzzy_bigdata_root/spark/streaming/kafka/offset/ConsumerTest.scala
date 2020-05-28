package gzzy_bigdata_root.spark.streaming.kafka.offset

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.internal.Logging
import gzzy_bigdata_root.spark.kafka.config.KafkaConfig

import scala.collection.JavaConversions._

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-07 20:59
  */
object ConsumerTest extends Serializable with Logging{

  def main(args: Array[String]): Unit = {
    val topic = "test8"
    val kafkaParams = KafkaConfig.getKafkaConfig("Kafka2esStreamingTest")
    //kafka的消费者代码
    //TODO 获取当前消费者组在kafka中的偏移，然后存储到redis中
    //获取偏移
    val consumer = new KafkaConsumer(kafkaParams)
    //訂閲主題
    consumer.subscribe(java.util.Arrays.asList(topic))
    consumer.poll(100)
    //获取分区
    val partitions = consumer.assignment()
    println("partitions"+partitions)
    //获取偏移信息
    consumer.seekToBeginning(partitions)
    consumer.pause(partitions)
    //保存到redis中的格式  key  groupId + topic   hash key值partion  value 偏移量
    val offsets = partitions.map(tp=>{
      tp.partition().toString-> consumer.position(tp).toString
    }).toMap

    println(offsets)
    //存储偏移
  }

}
