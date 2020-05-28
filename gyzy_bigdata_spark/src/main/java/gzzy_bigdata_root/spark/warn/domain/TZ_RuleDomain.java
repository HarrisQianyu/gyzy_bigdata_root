package gzzy_bigdata_root.spark.warn.domain;

import java.sql.Date;

/**
 * @author:
 * @description: 规则实体类
 * @Date:Created in 2019-04-25 00:47
 */
public class TZ_RuleDomain {

    private int id;
    private String warn_fieldname; //预警字段       phone
    private String warn_fieldvalue; //预警字段内容  18696666666
    private String publisher;      //发布者
    private String send_type;     //消息接收方式
    private String send_mobile;   //接收手机号
    private String send_mail;     //接收邮箱
    private String send_dingding; //接收钉钉
    private Date create_time;    //创建时间


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getWarn_fieldname() {
        return warn_fieldname;
    }

    public void setWarn_fieldname(String warn_fieldname) {
        this.warn_fieldname = warn_fieldname;
    }

    public String getWarn_fieldvalue() {
        return warn_fieldvalue;
    }

    public void setWarn_fieldvalue(String warn_fieldvalue) {
        this.warn_fieldvalue = warn_fieldvalue;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getSend_type() {
        return send_type;
    }

    public void setSend_type(String send_type) {
        this.send_type = send_type;
    }

    public String getSend_mobile() {
        return send_mobile;
    }

    public void setSend_mobile(String send_mobile) {
        this.send_mobile = send_mobile;
    }

    public String getSend_mail() {
        return send_mail;
    }

    public void setSend_mail(String send_mail) {
        this.send_mail = send_mail;
    }

    public String getSend_dingding() {
        return send_dingding;
    }

    public void setSend_dingding(String send_dingding) {
        this.send_dingding = send_dingding;
    }






































    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }
}
