package gzzy_bigdata_root.flume.service;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import gzzy_bigdata_root.common.regex.Validation;
import gzzy_bigdata_root.flume.constant.ErrorMapFields;
import gzzy_bigdata_root.flume.constant.MapFields;
import gzzy_bigdata_root.flume.intercptor.ELKinterceptor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-04 20:01
 */
public class DataValidation {
    private static final Logger LOG = Logger.getLogger(ELKinterceptor.class);
    private static final String USERNAME=ErrorMapFields.USERNAME;

    private static final String SJHM=ErrorMapFields.SJHM;
    private static final String SJHM_ERROR=ErrorMapFields.SJHM_ERROR;
    private static final String SJHM_ERRORCODE=ErrorMapFields.SJHM_ERRORCODE;

    private static final String QQ=ErrorMapFields.QQ;
    private static final String QQ_ERROR=ErrorMapFields.QQ_ERROR;
    private static final String QQ_ERRORCODE=ErrorMapFields.QQ_ERRORCODE;

    private static final String IMSI=ErrorMapFields.IMSI;
    private static final String IMSI_ERROR=ErrorMapFields.IMSI_ERROR;
    private static final String IMSI_ERRORCODE=ErrorMapFields.IMSI_ERRORCODE;

    private static final String IMEI=ErrorMapFields.IMEI;
    private static final String IMEI_ERROR=ErrorMapFields.IMEI_ERROR;
    private static final String IMEI_ERRORCODE=ErrorMapFields.IMEI_ERRORCODE;

    private static final String MAC=ErrorMapFields.MAC;
    private static final String CLIENTMAC=ErrorMapFields.CLIENTMAC;
    private static final String STATIONMAC=ErrorMapFields.STATIONMAC;
    private static final String BSSID=ErrorMapFields.BSSID;
    private static final String MAC_ERROR=ErrorMapFields.MAC_ERROR;
    private static final String MAC_ERRORCODE=ErrorMapFields.MAC_ERRORCODE;

    private static final String DEVICENUM=ErrorMapFields.DEVICENUM;
    private static final String DEVICENUM_ERROR=ErrorMapFields.DEVICENUM_ERROR;
    private static final String DEVICENUM_ERRORCODE=ErrorMapFields.DEVICENUM_ERRORCODE;

    private static final String CAPTURETIME=ErrorMapFields.CAPTURETIME;
    private static final String CAPTURETIME_ERROR=ErrorMapFields.CAPTURETIME_ERROR;
    private static final String CAPTURETIME_ERRORCODE=ErrorMapFields.CAPTURETIME_ERRORCODE;


    private static final String EMAIL=ErrorMapFields.EMAIL;
    private static final String EMAIL_ERROR=ErrorMapFields.EMAIL_ERROR;
    private static final String EMAIL_ERRORCODE=ErrorMapFields.EMAIL_ERRORCODE;

    private static final String AUTH_TYPE=ErrorMapFields.AUTH_TYPE;
    private static final String AUTH_TYPE_ERROR=ErrorMapFields.AUTH_TYPE_ERROR;
    private static final String AUTH_TYPE_ERRORCODE=ErrorMapFields.AUTH_TYPE_ERRORCODE;

    private static final String FIRM_CODE=ErrorMapFields.FIRM_CODE;
    private static final String FIRM_CODE_ERROR=ErrorMapFields.FIRM_CODE_ERROR;
    private static final String FIRM_CODE_ERRORCODE=ErrorMapFields.FIRM_CODE_ERRORCODE;

    private static final String STARTTIME=ErrorMapFields.STARTTIME;
    private static final String STARTTIME_ERROR=ErrorMapFields.STARTTIME_ERROR;
    private static final String STARTTIME_ERRORCODE=ErrorMapFields.STARTTIME_ERRORCODE;
    private static final String ENDTIME=ErrorMapFields.ENDTIME;
    private static final String ENDTIME_ERROR=ErrorMapFields.ENDTIME_ERROR;
    private static final String ENDTIME_ERRORCODE=ErrorMapFields.ENDTIME_ERRORCODE;


    private static final String LOGINTIME=ErrorMapFields.LOGINTIME;
    private static final String LOGINTIME_ERROR=ErrorMapFields.LOGINTIME_ERROR;
    private static final String LOGINTIME_ERRORCODE=ErrorMapFields.LOGINTIME_ERRORCODE;
    private static final String LOGOUTTIME=ErrorMapFields.LOGOUTTIME;
    private static final String LOGOUTTIME_ERROR=ErrorMapFields.LOGOUTTIME_ERROR;
    private static final String LOGOUTTIME_ERRORCODE=ErrorMapFields.LOGOUTTIME_ERRORCODE;

    public static Map<String,Object> dataValidation(Map<String,String> map){
        //数据清洗
        //验证完成之后，数据返回
        if(map == null){
            return null;
        }
        //存放异常信息的集合，map
        Map<String,Object> errorMap = new HashMap<>();
        //验证手机号码
        sjhmValidation(map,errorMap);
        //校验mac 判断mac是不是正确格式，统一大小写
        macValidation(map,errorMap);
        //经验度

        //TODO 大小写统一
        //TODO 时间类型统一
        //TODO 数据字段类型统一
        //TODO 数据修复  针对可以修复的数据进行修复
        //TODO 空字段
        return errorMap;
    }

    /**
     * 手机号码校验
     * @param map
     * @param errorMap
     */
    public static void sjhmValidation(Map<String,String> map,Map<String,Object> errorMap){
        //phone
        if(map.containsKey(MapFields.PHONE)){
            String sjhm = map.get(MapFields.PHONE);
            //正则验证  正则工具类
            boolean isMobile = Validation.isMobile(sjhm);
            if(!isMobile){
                LOG.error("===手机号码格式不对，号码为"+sjhm);
                errorMap.put(SJHM,sjhm);
                errorMap.put(SJHM_ERROR,SJHM_ERRORCODE);
            }
        }
    }

    /**
     * MAC校验
     * @param map
     * @param errorMap
     */
    public static void macValidation(Map<String,String> map,Map<String,Object> errorMap){
        //phone
        if(map.containsKey(MapFields.PHONE_MAC)){
            String mac = map.get(MapFields.PHONE_MAC);
            //正则验证  正则工具类
            if(StringUtils.isNotBlank(mac)){
                boolean bool = Validation.isMac(mac);
                if(!bool){
                    errorMap.put(MAC,mac);
                    errorMap.put(MAC_ERROR,MAC_ERRORCODE);
                }
            }else{
                LOG.error("MAC为空");
                errorMap.put(MAC,mac);
                errorMap.put(MAC_ERROR,MAC_ERRORCODE);
            }
        }
    }




}
