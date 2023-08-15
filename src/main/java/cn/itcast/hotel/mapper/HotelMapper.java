package cn.itcast.hotel.mapper;

import cn.itcast.hotel.pojo.Hotel;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * 酒店映射器
 *
 * @author liudo
 * @date 2023/08/15
 */
@Mapper
public interface HotelMapper extends BaseMapper<Hotel> {
}
