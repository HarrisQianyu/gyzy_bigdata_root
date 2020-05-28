package gzzy_bigdata_root.spark.common.convert

import java.util

import org.apache.spark.internal.Logging
import gzzy_bigdata_root.common.config.ConfigUtil

import scala.collection.JavaConversions._

/**
  * author: KING
  * description: RDD数据类型转换，将map[String,String]转为map[string,Object]
  * 转换需要按照mapping格式进行
  * Date:Created in 2020-03-09 22:15
  */
object DataConvert extends Serializable with Logging {

  //转换映射文件
  val FIELD_MAPPING_PATH = "es/mapping/fieldmapping.properties"

  private val tableFieldMap: util.HashMap[String, util.HashMap[String, String]] = getEsFieldtypeMap

  //转换
  def strMap2esObjectMap(map: java.util.Map[String, String]): java.util.Map[String, Object] = {
    //获取数据类型，根据数据类型获取映射关系
    val table = map.get("table")
    val fieldMappingMap = tableFieldMap.get(table)  //映射关系
    val objectMap = new java.util.HashMap[String, Object]

    //获取真实数据的所有key
    val iterator = map.keySet().iterator()
    while (iterator.hasNext){
      val field = iterator.next()
      //通过字段获取类型 string
      val dataType = fieldMappingMap.get(field)
      dataType match {
        case "long"=> BaseDataConvert.mapString2Long(map,field,objectMap)
        case "string"=> BaseDataConvert.mapString2String(map,field,objectMap)
        case "double"=> BaseDataConvert.mapString2Double(map,field,objectMap)
        case  _=> BaseDataConvert.mapString2String(map,field,objectMap)
      }
    }
    objectMap
  }


  def main(args: Array[String]): Unit = {
    val stringToStringToString = DataConvert.getEsFieldtypeMap()
    stringToStringToString.foreach(map => {
      println(map._1)
      println(map._2)
    })
  }


  //解析配置文件
  def getEsFieldtypeMap(): java.util.HashMap[String, java.util.HashMap[String, String]] = {
    //TODO 解析为一个MAP，可以方便的通过(table)类型 获取映射关系
    /*    (
           "wechat":{"rksj"->"long","imei" ->"string"}
      "mail":{"rksj"->"long","imei" ->"string"}
      "qq":{"rksj"->"long","imei" ->"string"}
        )*/

    val mapMap = new java.util.HashMap[String, java.util.HashMap[String, String]]()
    //读取映射配置文件
    val properties = ConfigUtil.getInstance().getProperties(FIELD_MAPPING_PATH)
    //获取到所有的key
    val tableFields = properties.keySet()
    //获取所有的类型
    val tables = properties.get("tables").toString.split(",")

    //使用类型进行过滤，将相同类型的key放到一起
    tables.foreach(table => {
      val map = new java.util.HashMap[String, String]
      tableFields.foreach(tableField => {
        if (tableField.toString.startsWith(table)) {
          //获取真实key
          val key = tableField.toString.split("\\.")(1)
          val value = properties.get(tableField).toString
          map.put(key, value)
        }
      })
      mapMap.put(table, map)
    })
    mapMap
  }

}
