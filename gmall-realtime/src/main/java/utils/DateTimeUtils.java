package utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;


// 没有多线程问题
public class DateTimeUtils {
    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将Date类型格式化为时间字符串
     *
     * @param date
     * @return
     */
    public static String toDate(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formatter.format(localDateTime);
    }

    /**
     * 将时间字符串解析成时间戳
     *
     * @param ts
     * @return
     */
    public static Long toTs(String ts) {
        LocalDateTime localDateTime = LocalDateTime.parse(ts, formatter);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
