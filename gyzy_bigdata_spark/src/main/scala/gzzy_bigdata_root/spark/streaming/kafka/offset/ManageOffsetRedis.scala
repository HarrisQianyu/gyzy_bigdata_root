package gzzy_bigdata_root.spark.streaming.kafka.offset

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.internals.Topic
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import gzzy_bigdata_root.redis.client.JedisUtil

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
  * author: KING
  * description:
  * Date:Created in 2020-03-06 21:05
  */
object ManageOffsetRedis {

  /**
    *
    * @param db   redis 库名
    * @param groupId   消费者组ID
    * @param offsetRanges  spark中的偏移信息
    */
  def saveOffsetToRedis(db: Int, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis = JedisUtil.getJedis(db)
    offsetRanges.foreach(offsetRange=>{
       val redisKey = s"${groupId}_${offsetRange.topic}"
       jedis.hset(redisKey,offsetRange.partition.toString,offsetRange.untilOffset.toString)
    })
    JedisUtil.close(jedis)
  }


  def getOffsetFromRedis(db: Int,groupId: String,topic: String,kafkaParams:java.util.Map[String,String]): java.util.Map[String,String] ={
    val jedis = JedisUtil.getJedis(db)
    val redisKey = s"${groupId}_${topic}"
    var offsetMap = jedis.hgetAll(redisKey)
    //TODO 如果第一次跑，redis为空怎么处理
    if(offsetMap.size() == 0){
        //TODO 将kafka中最早的偏移初始化到redis中
      offsetMap = initOffset2Redis(topic,kafkaParams.asInstanceOf[java.util.Map[String,Object]])
    }
    jedis.close()
    offsetMap
  }


  /**
    * 初始化redis中的偏移
    * @param topic
    * @param kafkaParams
    */
  def initOffset2Redis(topic: String,kafkaParams:java.util.Map[String,Object]): java.util.Map[String,String] ={
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
    consumer.unsubscribe()
    consumer.close()
    offsets
  }



  def updateOffset[T:ClassTag](db: Int, groupId: String, rdd:RDD[T]): Unit ={
    val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    saveOffsetToRedis(db,groupId,offsetRanges)
  }


}
