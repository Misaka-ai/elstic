package cn.itcast.hotel.service;

import cn.itcast.hotel.common.PageResult;
import cn.itcast.hotel.dto.HotelPageQueryDTO;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.vo.HotelVO;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * 酒店服务
 *
 * @author liudo
 * @date 2023/08/15
 */
public interface HotelService extends IService<Hotel> {
    /*
    * 分页查询
    * */
    PageResult<HotelVO> listData(HotelPageQueryDTO hotelPageQueryDTO);
}
