package cn.itcast.hotel.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * @author liudo
 * @version 1.0
 * @project hotel-demo
 * @description
 * @date 2023/8/20 16:34:35
 */
@Data
public class HotelVO {
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

    /**
     * 距离
     */
    private Object distance;

    /**
     * 是广告
     */
    @JsonProperty(value = "isAD")
    private boolean isAD;
}
