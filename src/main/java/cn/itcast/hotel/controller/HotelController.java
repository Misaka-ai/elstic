package cn.itcast.hotel.controller;

import cn.itcast.hotel.common.PageResult;
import cn.itcast.hotel.dto.HotelPageQueryDTO;
import cn.itcast.hotel.service.HotelService;
import cn.itcast.hotel.vo.HotelVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hotel")
@Slf4j
@RequiredArgsConstructor
public class HotelController {

    private final HotelService hotelService;

    @PostMapping("/list")
    public PageResult<HotelVO> list(@RequestBody HotelPageQueryDTO hotelPageQueryDTO) {
        return hotelService.listData(hotelPageQueryDTO);
    }
}
