package bean;

import java.sql.Timestamp;

/**
 * @author lihaoran
 */
public class User {
    private String name;
    private Double temperature;
    private Long time;

    public User(String name, Double temperature, Long time) {
        this.name = name;
        this.temperature = temperature;
        this.time = time;
    }

    public User() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }



    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", temperature=" + temperature +
                ", time=" + time +
                '}';
    }
}
