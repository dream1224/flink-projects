package bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class OrderDetail extends BaseBean {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;

    @Override
    public Long pickLastUpdateTime() {
        return this.create_ts;
    }

    @Override
    public String pickRowKey() {
        return this.id.toString();
    }
}
