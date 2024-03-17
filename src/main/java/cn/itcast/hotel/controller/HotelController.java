package cn.itcast.hotel.controller;

import cn.itcast.hotel.common.PageResult;
import cn.itcast.hotel.dto.HotelPageQueryDTO;
import cn.itcast.hotel.service.HotelService;
import cn.itcast.hotel.vo.HotelVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

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
    @PostMapping("/filters")
    public Map<String, List<String>> filters(@RequestBody HotelPageQueryDTO hotelPageQueryDTO){
        return hotelService.filters(hotelPageQueryDTO);
    }

    @GetMapping("/suggestion")
    public List<String> suggestion(String key){
        return hotelService.suggestion(key);
    }
}
