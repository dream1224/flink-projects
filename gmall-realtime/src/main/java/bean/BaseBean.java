package bean;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;

import java.util.List;

public abstract class BaseBean {
    private static final Logger logger = LoggerFactory.getLogger(BaseBean.class.getSimpleName());

    public abstract Long pickLastUpdateTime();

    public abstract String pickRowKey();

    public List<String> pickFieldName() {
        ArrayList<String> fieldList = new ArrayList<>();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            fieldList.add(field.getName());
        }
        return fieldList;
    }


}
