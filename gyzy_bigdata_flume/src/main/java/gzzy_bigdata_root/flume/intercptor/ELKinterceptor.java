package gzzy_bigdata_root.flume.intercptor;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;
import gzzy_bigdata_root.flume.constant.ConstantFields;
import gzzy_bigdata_root.flume.service.DataCheck;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-02 21:39
 */
public class ELKinterceptor implements Interceptor {

    private static final Logger LOG = Logger.getLogger(ELKinterceptor.class);
    @Override
    public Event intercept(Event event) {

        if(event ==null){
           return null;
        }

        //TODO 获取并解析event中的信息  header  body
        Map<String, String> headers = event.getHeaders();
        String fileName = headers.get(ConstantFields.FILE_NAME);
        String absolute_filename = headers.get(ConstantFields.ABSOLUTE_FILENAME);
        //获取body
        String line = new String(event.getBody(),Charsets.UTF_8);
        //TODO 进行数据清洗
        Map map = DataCheck.txtParseAndValidation(fileName, absolute_filename, line);
        if(map == null){
            return null;
        }
        String json = JSON.toJSONString(map);
        LOG.info("拦截器执行map转换json=》" + line);
        Event eventNew = new SimpleEvent();
        eventNew.setBody(json.getBytes());
        return eventNew;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> listEvents = new ArrayList<>();
        list.forEach(event->{
            Event intercept = intercept(event);
            if(intercept !=null){
                listEvents.add(intercept);
            }
        });
        return listEvents;
    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new ELKinterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Override
    public void close() {

    }


    @Override
    public void initialize() {

    }
}
