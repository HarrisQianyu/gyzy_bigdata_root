package gzzy_bigdata_root.spark.warn.domain;

import java.sql.Date;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-25 05:25
 */
public class WarningMessage {
    private String id;   //主键id
    private String alarmRuleid;   //规则id
    private String alarmType;     //告警类型
    private String sendType;      //发送方式
    private String sendMobile;    //发送至手机
    private String sendEmail;     //发送至邮箱
    private String sendStatus;    //发送状态
    private String senfInfo;      //发送内容
    private Date hitTime;         //命中时间
    private Date checkinTime;     //入库时间
    private String isRead;        //是否已读
    private String readAccounts;  //已读用户
    private String alarmaccounts;
    private String accountid;    //规则发布人

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAlarmRuleid() {
        return alarmRuleid;
    }

    public void setAlarmRuleid(String alarmRuleid) {
        this.alarmRuleid = alarmRuleid;
    }

    public String getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(String alarmType) {
        this.alarmType = alarmType;
    }

    public String getSendType() {
        return sendType;
    }

    public void setSendType(String sendType) {
        this.sendType = sendType;
    }

    public String getSendMobile() {
        return sendMobile;
    }

    public void setSendMobile(String sendMobile) {
        this.sendMobile = sendMobile;
    }

    public String getSendEmail() {
        return sendEmail;
    }

    public void setSendEmail(String sendEmail) {
        this.sendEmail = sendEmail;
    }

    public String getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(String sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getSenfInfo() {
        return senfInfo;
    }

    public void setSenfInfo(String senfInfo) {
        this.senfInfo = senfInfo;
    }

    public Date getHitTime() {
        return hitTime;
    }

    public void setHitTime(Date hitTime) {
        this.hitTime = hitTime;
    }

    public Date getCheckinTime() {
        return checkinTime;
    }

    public void setCheckinTime(Date checkinTime) {
        this.checkinTime = checkinTime;
    }

    public String getIsRead() {
        return isRead;
    }

    public void setIsRead(String isRead) {
        this.isRead = isRead;
    }

    public String getReadAccounts() {
        return readAccounts;
    }

    public void setReadAccounts(String readAccounts) {
        this.readAccounts = readAccounts;
    }

    public String getAlarmaccounts() {
        return alarmaccounts;
    }

    public void setAlarmaccounts(String alarmaccounts) {
        this.alarmaccounts = alarmaccounts;
    }

    public String getAccountid() {
        return accountid;
    }

    public void setAccountid(String accountid) {
        this.accountid = accountid;
    }

    @Override
    public String toString() {
        return "WarningMessage{" +
                "id='" + id + '\'' +
                ", alarmRuleid='" + alarmRuleid + '\'' +
                ", alarmType='" + alarmType + '\'' +
                ", sendType='" + sendType + '\'' +
                ", sendMobile='" + sendMobile + '\'' +
                ", sendEmail='" + sendEmail + '\'' +
                ", sendStatus='" + sendStatus + '\'' +
                ", senfInfo='" + senfInfo + '\'' +
                ", hitTime=" + hitTime +
                ", checkinTime=" + checkinTime +
                ", isRead='" + isRead + '\'' +
                ", readAccounts='" + readAccounts + '\'' +
                ", alarmaccounts='" + alarmaccounts + '\'' +
                ", accountid='" + accountid + '\'' +
                '}';
    }
}
