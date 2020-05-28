package gzzy_bigdata_root.spark.streaming.kafka.kafka2es

import com.alibaba.fastjson.{JSON, TypeReference}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import gzzy_bigdata_root.spark.streaming.kafka.Spark_Es_ConfigUtil
import gzzy_bigdata_root.spark.streaming.kafka.offset.ManageOffsetRedis

import scala.collection.JavaConversions._

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-04 22:16
  */
object Test extends Serializable with Logging{

  def main(args: Array[String]): Unit = {
      val topic ="test8"
      val groupId = "Kafka2esStreamingTest"
      val sparkConf = new SparkConf().setAppName("Kafka2esStreamingTest").setMaster("local[2]")
      val ssc = new StreamingContext(sparkConf,Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "had-11:9092,had-12:9092,had-13:9092,had-14:9092,had-15:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //还没有从redis中获取偏移，还是从0开始读
    //需要从redis中获取偏移
    val offsetMap = ManageOffsetRedis.getOffsetFromRedis(1,groupId,topic,kafkaParams.asInstanceOf[java.util.Map[String,String]])
    offsetMap.foreach(x=>{
      s"初始读取到的offset:${x}"
    })

    //使用redis中的类型进行类型转换
    val offsets = offsetMap.map(offset=>{
       new TopicPartition(topic,offset._1.toInt)->offset._2.toLong
    }).toMap


    val DS = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String,String](offsets.keys.toList,kafkaParams,offsets))


   /* val DS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set(topic), kafkaParams))*/

    DS.foreachRDD(rdd=>{

      //TODO 可以从RDD中获取到当前消费者的offset
      //TODO 偏移信息我们保存到REDIS中去
      //获取当前消费者的偏移
      val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offset=>{


        println("==============================")
        println(offset.partition)
        println(offset.fromOffset)
        println(offset.untilOffset)
      })


      val mapRDD = rdd.map(consumerRecourd=>{
        val line = consumerRecourd.value()
        val map = JSON.parseObject(line,new TypeReference[java.util.Map[String,String]](){})
        map
      })
      //第三方 ES
      EsSpark.saveToEs(mapRDD,"test8/test8",Spark_Es_ConfigUtil.getEsParam("id"))

      ManageOffsetRedis.saveOffsetToRedis(1,"Kafka2esStreamingTest",offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
