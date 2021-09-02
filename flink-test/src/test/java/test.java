public class test {
    public static void main(String[] args) {
        String str1 = "ABC";
        String str2 = "5093";
        String str3 = "公共设施";

//        String str = str1 + "\1" + str2 + "\1" + str3;
//        System.out.println(str);
        // 也就是下面的字符串，分隔符为 \u0001
        String str = "ABC\u00015093\u0001公共设施";
        String[] split = str.split("\1");
        for (String s : split) {
            System.out.println(s);
        }
        System.out.println("数组长度:" + split.length);
    }
}
