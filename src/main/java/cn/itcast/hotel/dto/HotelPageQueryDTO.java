package cn.itcast.hotel.dto;

import lombok.Data;

/**
 * 酒店页面查询d去
 * 列出酒店d去
 *
 * @author liudo
 * @version 1.0
 * @project hotel-demo
 * @description
 * @date 2023/8/20 14:30:25
 */
@Data
public class HotelPageQueryDTO {

    /**
     * 钥匙
     */
    private String key;

    /**
     * 页面
     */
    private Integer page;

    /**
     * 尺寸
     */
    private Integer size;

    /**
     * 排序依据
     */
    private String sortBy;

    /**
     * 城市
     */
    private String city;

    /**
     * 品牌
     */
    private String brand;

    /**
     * 星际
     */
    private String starName;

    /**
     * 最低价格
     */
    private Integer minPrice;

    /**
     * 最高价格
     */
    private Integer maxPrice;

    /**
     * 位置
     */
    private String location;
}
