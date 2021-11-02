import lombok.var;
import net.minidev.json.writer.MapperRemapped;

import java.lang.reflect.Field;
import java.util.*;

public class HashMapTest {
    public static void main(String[] args) throws Exception {
        Map<String, User> userMap = new HashMap<>();
        User user1 = new User("1001", "1,2");
        User user2 = new User("1002", "1");
        userMap.put("a", user1);
        userMap.put("b", user2);
        Set<String> strings = userMap.keySet();
        String[] strings1 = strings.toArray(new String[strings.size()]);
        HashMap<String, Customer> customerMap = new HashMap<>();
        Customer customer1 = new Customer("c1", "zs");
        Customer customer2 = new Customer("c2", "ls");
        Customer customer3 = new Customer("c3", "ww");
        customerMap.put("1", customer1);
        customerMap.put("2", customer2);
        customerMap.put("3", customer2);
        if (userMap.size() > 1) {
            Map<String, Customer> name = IndexedTable.getIndexedTable(userMap, customerMap, strings1, "name");
            System.out.println(name.toString());
        }
    }
}

