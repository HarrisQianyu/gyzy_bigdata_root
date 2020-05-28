package gzzy_bigdata_root.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2020-02-22 21:29
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String myProp;

    //读取flume配置文件  flume.conf
    @Override
    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");
        this.myProp = myProp;
    }


    @Override
    public Status process() throws EventDeliveryException {
        //所有的业务逻辑直接在这里
        //TODO ■ 实时解析处理传送过来的FTP格式WIFI数据(txt)
        //TODO ■ FTP文件备份(数据备份，数据重跑)  最开始的源头是wifi设备。
        //TODO ■ FTP异常文件备份(查错和重跑使用)
        Status status = null;
        try {
            Event e = new SimpleEvent();

            getChannelProcessor().processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
        }
        return status;
    }


    @Override
    public void start() {
    }

    @Override
    public void stop () {
    }


    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}