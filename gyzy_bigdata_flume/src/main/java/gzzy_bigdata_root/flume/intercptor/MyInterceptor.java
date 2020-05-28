package gzzy_bigdata_root.flume.intercptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-03-02 21:35
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    //拦截方法
    @Override
    public Event intercept(Event event) {

        //具体的实现逻辑

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        list.forEach(event->{
            intercept(event);
        });
        return null;
    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Override
    public void close() {

    }
}
