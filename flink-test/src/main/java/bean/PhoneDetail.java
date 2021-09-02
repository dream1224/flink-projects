package bean;

/**
 * @author lihaoran
 */
public class PhoneDetail {
    private String phoneName;
    private Double price;

    public PhoneDetail() {
    }

    public PhoneDetail(String phoneName, Double price) {
        this.phoneName = phoneName;
        this.price = price;
    }

    public String getPhoneName() {
        return phoneName;
    }

    public void setPhoneName(String phoneName) {
        this.phoneName = phoneName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }



    @Override
    public String toString() {
        return "PhoneDetail{" +
                "phoneName='" + phoneName + '\'' +
                ", price=" + price +
                '}';
    }
}
