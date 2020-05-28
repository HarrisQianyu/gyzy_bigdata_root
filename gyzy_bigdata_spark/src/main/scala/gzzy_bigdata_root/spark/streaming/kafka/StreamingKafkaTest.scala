package gzzy_bigdata_root.spark.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-04 22:16
  */
object StreamingKafkaTest {

  def main(args: Array[String]): Unit = {
      val topic ="test8"
      val groupId = "StreamingKafkaTest"
      val sparkConf = new SparkConf().setAppName("StreamingKafkaTest").setMaster("local[2]")
      val ssc = new StreamingContext(sparkConf,Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "had-11:9092,had-12:9092,had-13:9092,had-14:9092,had-15:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val DS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set(topic), kafkaParams))

    DS.foreachRDD(rdd=>{
      rdd.foreach(line =>{
        println(line)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
