package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.common.PageResult;
import cn.itcast.hotel.dto.HotelPageQueryDTO;
import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.service.HotelService;
import cn.itcast.hotel.vo.HotelVO;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 酒店服务
 *
 * @author liudo
 * @date 2023/08/15
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements HotelService {
    public static final String HOTEL = "hotel";
    private final RestHighLevelClient restHighLevelClient;

    @Override
    public PageResult<HotelVO> listData(HotelPageQueryDTO hotelPageQueryDTO) {


        String key = hotelPageQueryDTO.getKey();
        int page = Objects.isNull(hotelPageQueryDTO.getPage()) ? 1 : hotelPageQueryDTO.getPage();
        int size = Objects.isNull(hotelPageQueryDTO.getSize()) ? 20 : hotelPageQueryDTO.getSize();


        String city = hotelPageQueryDTO.getCity();
        String brand = hotelPageQueryDTO.getBrand();
        String starName = hotelPageQueryDTO.getStarName();
        Integer minPrice = hotelPageQueryDTO.getMinPrice();
        Integer maxPrice = hotelPageQueryDTO.getMaxPrice();
        String location = hotelPageQueryDTO.getLocation();
        int from = (page - 1) * size;
        SearchRequest searchRequest = new SearchRequest(HOTEL);

        try {

            BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilder(key, city, starName, brand, minPrice, maxPrice);
            //1.原始查询 boolQueryBuilder
            //2.广告过滤
            //3.10
            //4.sum
            searchRequest.source()
                    .query(
                            new FunctionScoreQueryBuilder(
                                    boolQueryBuilder,
                                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                            new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                                    QueryBuilders.termQuery("brand", "如家"),
                                                    ScoreFunctionBuilders.weightFactorFunction(10)
                                            )
                                    }
                            )
                                    .boostMode(CombineFunction.SUM)
                    )
                    .from(from)
                    .size(size);

            if (StringUtils.isNoneEmpty(location)) {
                searchRequest.source()
                        .sort(
                                SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                                        .unit(DistanceUnit.KILOMETERS)
                                        .order(SortOrder.ASC)
                        );
            }
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits = searchResponse.getHits();
            TotalHits totalHits = searchHits.getTotalHits();
            long value = totalHits.value;

            SearchHit[] hits = searchHits.getHits();
            List<HotelVO> hotelVOList = Arrays.stream(hits).map(hit -> {
                        String sourceAsString = hit.getSourceAsString();
                        HotelVO hotelVO = JSON.parseObject(sourceAsString, HotelVO.class);
                        Object[] sortValues = hit.getSortValues();

                        if (sortValues.length > 0) {
                            hotelVO.setDistance(sortValues[0]);
                        }
                        hotelVO.setAD(Objects.equals("如家", hotelVO.getBrand()));
                        return hotelVO;
                    })
                    .collect(Collectors.toList());
            PageResult<HotelVO> hotelVOPageResult = new PageResult<>();
            hotelVOPageResult.setData(hotelVOList);
            hotelVOPageResult.setTotal(value);
            return hotelVOPageResult;

        } catch (IOException e) {
            log.info(e.getMessage(), e);
        }
        return null;
    }

    private static BoolQueryBuilder getBoolQueryBuilder(String key, String city, String starName, String brand, Integer minPrice, Integer maxPrice) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (StringUtils.isNoneEmpty(key)) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", key));
        } else {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }
        //城市
        if (StringUtils.isNoneEmpty(city)) {
            boolQueryBuilder.filter(
                    QueryBuilders.termQuery("city", city)
            );
        }
        //星级
        if (StringUtils.isNoneEmpty(starName)) {
            boolQueryBuilder.filter(
                    QueryBuilders.termQuery("starName", starName)
            );
        }
        //品牌
        if (StringUtils.isNoneEmpty(brand)) {
            boolQueryBuilder.filter(
                    QueryBuilders.termQuery("brand", brand)
            );
        }
        //价格区间
        if (Objects.nonNull(minPrice) && Objects.nonNull(minPrice)) {
            boolQueryBuilder.filter(
                    QueryBuilders.rangeQuery("price")
                            .gte(minPrice)
                            .lte(maxPrice)
            );
        }
        return boolQueryBuilder;
    }

    @Override
    public Map<String, List<String>> filters(HotelPageQueryDTO hotelPageQueryDTO) {
        String key = hotelPageQueryDTO.getKey();
        String city = hotelPageQueryDTO.getCity();
        String brand = hotelPageQueryDTO.getBrand();
        String starName = hotelPageQueryDTO.getStarName();
        Integer minPrice = hotelPageQueryDTO.getMinPrice();
        Integer maxPrice = hotelPageQueryDTO.getMaxPrice();

        SearchRequest searchRequest = new SearchRequest(HOTEL);

        String brandAgg = "brandAgg";
        String cityAgg = "cityAgg";
        String starNameAgg = "starNameAgg";
        searchRequest.source()
                .query(
                        getBoolQueryBuilder(key, city, starName, brand, minPrice, maxPrice)
                )
                .aggregation(AggregationBuilders.terms(brandAgg)
                        .field("brand")
                        .size(10)
                )
                .aggregation(AggregationBuilders.terms(cityAgg)
                        .field("city")
                        .size(10)
                )
                .aggregation(AggregationBuilders.terms(starNameAgg)
                        .field("starName")
                        .size(10)
                );

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms brandAggGregation = aggregations.get(brandAgg);
        ParsedStringTerms cityAggGregation = aggregations.get(cityAgg);
        ParsedStringTerms starNameAggGregation = aggregations.get(starNameAgg);


        List<String> brands = brandAggGregation.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString)
                .collect(Collectors.toList());

        List<String> starNames = starNameAggGregation.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString)
                .collect(Collectors.toList());

        List<String> citys = cityAggGregation.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString)
                .collect(Collectors.toList());


        HashMap<String, List<String>> stringListHashMap = new HashMap<>();
        stringListHashMap.put("brand", brands);
        stringListHashMap.put("starName", starNames);
        stringListHashMap.put("city", citys);

        return stringListHashMap;
    }

    @Override
    public List<String> suggestion(String key) {
        if (StringUtils.isEmpty(key)) {
            return new ArrayList<>();
        }
        SearchRequest searchRequest = new SearchRequest(HOTEL);
        searchRequest.source()
                .suggest(
                        new SuggestBuilder()
                                .addSuggestion("suggestionSugg"
                                        , SuggestBuilders.completionSuggestion("suggestion")
                                                .prefix(key)
                                                .skipDuplicates(true)
                                                .size(10)
                                )
                );

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.info(e.getMessage(), e);
        }
        Suggest suggest = searchResponse.getSuggest();
        CompletionSuggestion suggestionSuggest = suggest.getSuggestion("suggestionSugg");
        return suggestionSuggest.getOptions().stream()
                .map(option -> option.getText().string())
                .collect(Collectors.toList());
    }

}

