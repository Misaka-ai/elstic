package cn.itcast.hotel;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.RareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest

public class AggTest {
    public static final String HOTEL = "hotel";
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testCase01() throws IOException {

        SearchRequest searchRequest = new SearchRequest(HOTEL);

        String brandAgg = "brandAgg";
        searchRequest.source()
                .size(0)
                .aggregation(
                        AggregationBuilders.terms(brandAgg)
                                .field("brand")
                                .size(10)
                );

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms aggregation = aggregations.get(brandAgg);
        List<? extends Terms.Bucket> buckets = aggregation.getBuckets();

        for (Terms.Bucket bucket : buckets) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }
    }

    @Test
    void testCase02() throws IOException {

        SearchRequest searchRequest = new SearchRequest(HOTEL);

        String brandAgg = "brandAgg";
        searchRequest.source()
                .size(0)
                .aggregation(
                        AggregationBuilders.terms(brandAgg)
                                .field("brand")
                                .size(10)
                                .order(BucketOrder.count(true))
                );

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms aggregation = aggregations.get(brandAgg);
        List<? extends Terms.Bucket> buckets = aggregation.getBuckets();

        for (Terms.Bucket bucket : buckets) {
            long docCount = bucket.getDocCount();
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString() + docCount);
        }
    }

    @Test
    void testCase03() throws IOException {

        SearchRequest searchRequest = new SearchRequest(HOTEL);

        String brandAgg = "brandAgg";
        String cityAgg = "cityAgg";
        String starNameAgg = "starNameAgg";
        searchRequest.source()
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

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);


        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms brandAggGregation = aggregations.get(brandAgg);
        ParsedStringTerms cityAggGregation = aggregations.get(cityAgg);
        ParsedStringTerms starNameAggGregation = aggregations.get(starNameAgg);


        for (Terms.Bucket bucket : brandAggGregation.getBuckets()) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }
        System.out.println("---------------------------------------------");
        for (Terms.Bucket bucket : cityAggGregation.getBuckets()) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }
        System.out.println("---------------------------------------------");
        for (Terms.Bucket bucket : starNameAggGregation.getBuckets()) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }


    }

    @Test
    void testCase04() throws IOException {

        SearchRequest searchRequest = new SearchRequest(HOTEL);

        String brandAgg = "brandAgg";
        String cityAgg = "cityAgg";
        String starNameAgg = "starNameAgg";
        searchRequest.source()
                .query(
                        QueryBuilders.matchQuery("all", "如家")
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

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);


        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms brandAggGregation = aggregations.get(brandAgg);
        ParsedStringTerms cityAggGregation = aggregations.get(cityAgg);
        ParsedStringTerms starNameAggGregation = aggregations.get(starNameAgg);


        for (Terms.Bucket bucket : brandAggGregation.getBuckets()) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }
        System.out.println("---------------------------------------------");
        for (Terms.Bucket bucket : cityAggGregation.getBuckets()) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }
        System.out.println("---------------------------------------------");
        for (Terms.Bucket bucket : starNameAggGregation.getBuckets()) {
            System.out.println("bucket.getKeyAsString() = " + bucket.getKeyAsString());
        }


    }

}
