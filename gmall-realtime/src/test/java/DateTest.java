import utils.DateTimeUtils;

import java.util.Date;

public class DateTest {
    public static void main(String[] args) {
        Date date = new Date();
        String s = DateTimeUtils.toDate(date);
        System.out.println(date);
        System.out.println(s);
        Long aLong = DateTimeUtils.toTs(s);
        System.out.println(aLong);
    }
}
