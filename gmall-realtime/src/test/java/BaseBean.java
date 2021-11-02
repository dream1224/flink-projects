import java.lang.reflect.Method;

public class BaseBean {
    public Object getValueVidFieldName(String fieldName) throws Exception {
        String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        Method fieldMethod = this.getClass().getDeclaredMethod(methodName);
        Object result = fieldMethod.invoke(this);
        return result;
    }
}
