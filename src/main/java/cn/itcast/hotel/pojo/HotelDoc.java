package cn.itcast.hotel.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 酒店医生
 *
 * @author liudo
 * @date 2023/08/15
 */
@Data
@NoArgsConstructor
public class HotelDoc {
    /**
     * id
     */
    private Long id;
    /**
     * 名字
     */
    private String name;
    /**
     * 地址
     */
    private String address;
    /**
     * 价格
     */
    private Integer price;
    /**
     * 分数
     */
    private Integer score;
    /**
     * 品牌
     */
    private String brand;
    /**
     * 城市
     */
    private String city;
    /**
     * 明星名字
     */
    private String starName;
    /**
     * 业务
     */
    private String business;
    /**
     * 位置
     */
    private String location;
    /**
     * 图片
     */
    private String pic;

    public HotelDoc(Hotel hotel) {
        this.id = hotel.getId();
        this.name = hotel.getName();
        this.address = hotel.getAddress();
        this.price = hotel.getPrice();
        this.score = hotel.getScore();
        this.brand = hotel.getBrand();
        this.city = hotel.getCity();
        this.starName = hotel.getStarName();
        this.business = hotel.getBusiness();
        this.location = hotel.getLatitude() + ", " + hotel.getLongitude();
        this.pic = hotel.getPic();
    }
}
