package gzzy_bigdata_root.spark.warn.timer;

import gzzy_bigdata_root.spark.warn.domain.WarningMessage;

/**
 * @author: KING
 * @description: 短信告警
 * @Date:Created in 2020-03-13 20:55
 */
public class PhoneWarnImpl implements WarnI{
    @Override
    public boolean warn(WarningMessage warningMessage) {

        //首先获取手机号码
        String[] phoneNums = warningMessage.getSendMobile().split(",");
        //获取告警内容
        String sendInfo = warningMessage.getSenfInfo();

        for (int i = 0; i <phoneNums.length ; i++) {
            String phoneNum =  phoneNums[i];
            MessageSend.sendMessage(phoneNum,sendInfo);
        }

        return false;
    }
}
