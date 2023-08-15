package cn.itcast.hotel.pojo;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/**
 * 酒店
 *
 * @author liudo
 * @date 2023/08/15
 */
@Data
@TableName("tb_hotel")
public class Hotel {
    /**
     * id
     */
    @TableId(type = IdType.INPUT)
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
     * 经度
     */
    private String longitude;
    /**
     * 纬度
     */
    private String latitude;
    /**
     * 图片
     */
    private String pic;
}
