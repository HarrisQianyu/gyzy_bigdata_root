package gzzy_bigdata_root.spark.warn.timer;

import org.apache.log4j.Logger;

/**
 * @author: KING
 * @description: 模拟短信接口，微信接口，钉钉接口
 * @Date:Created in 2020-03-13 20:48
 */
public class MessageSend {

    private static final Logger LOG = Logger.getLogger(MessageSend.class);

    /**
     * 模拟短信接口
     * @param phoneNum
     * @param sendInfo
     */
    public static void sendMessage(String phoneNum,String sendInfo){
        System.out.println("发送短信到手机号为"+phoneNum);
        System.out.println("发送内容为"+sendInfo);
    }

    /**
     * 模拟短信接口
     * @param dingNum
     * @param sendInfo
     */
    public static void sendDDMessage(String dingNum,String sendInfo){
        System.out.println("发送信息到钉钉号为"+dingNum);
        System.out.println("发送内容为"+sendInfo);
    }

}
