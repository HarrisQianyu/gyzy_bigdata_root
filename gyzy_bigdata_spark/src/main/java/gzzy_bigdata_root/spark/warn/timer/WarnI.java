package gzzy_bigdata_root.spark.warn.timer;

import gzzy_bigdata_root.spark.warn.domain.WarningMessage;

/**
 * @author: KING
 * @description: 告警接口
 * @Date:Created in 2020-03-13 20:53
 */
public interface WarnI {

    boolean warn(WarningMessage warningMessage);
}
