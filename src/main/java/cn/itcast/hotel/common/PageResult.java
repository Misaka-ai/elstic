package cn.itcast.hotel.common;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liudo
 * @version 1.0
 * @project hotel-demo
 * @description
 * @date 2023/8/20 14:32:38
 */
@Data
public class PageResult<T> {

    private Long total = 0L;

    private List<T> data = new ArrayList<>();
}
