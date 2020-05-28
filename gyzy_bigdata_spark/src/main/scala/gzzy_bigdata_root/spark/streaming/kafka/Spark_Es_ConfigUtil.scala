package gzzy_bigdata_root.spark.streaming.kafka

/**
  * @author: KING
  * @description:
  * @Date:Created in 2020-03-06 20:17
  */
object Spark_Es_ConfigUtil {

   def getEsParam(idField:String): Map[String,String] ={
      Map[String,String]("es.mapping.id"->idField, //使用map中的什么字段作为ES的主键
        "es.nodes"->"had-12,had-13,had-14,had-15",
        "es.port"->"9200",
        "es.clustername"->"hlx_es"
      )
   }

}
