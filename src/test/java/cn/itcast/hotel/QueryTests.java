package cn.itcast.hotel;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@SpringBootTest
public class QueryTests {
    public static final String HOTEL = "hotel";
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testMach() throws IOException {
        SearchRequest requsest = new SearchRequest(HOTEL);
        requsest.source().query(
                QueryBuilders.matchQuery("name", "如家")
        );

        SearchResponse searchResponse = restHighLevelClient.search(requsest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hitsHits = searchHits.getHits();
        for (SearchHit hitsHit : hitsHits) {
            String sourceAsString = hitsHit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }
    }

    @Test
    void testMatch2() throws IOException {
        //查询具体
        SearchRequest request = new SearchRequest(HOTEL);
        SearchSourceBuilder price = request.source().query(
                QueryBuilders.termQuery("price", 159)
        );


        SearchResponse searchResponse = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }
    }

    @Test
    void testMatch() throws IOException {
        //查询范围

        SearchRequest request = new SearchRequest(HOTEL);
        request.source().query(
                QueryBuilders.rangeQuery("price")
                        .gte(100)
                        .lte(500)
        );
        SearchResponse searchResponse = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }

    }

    @Test
    void testMatch3() throws IOException {
        SearchRequest request = new SearchRequest(HOTEL);
        request.source().query(
                QueryBuilders.boolQuery()
                        .must(
                                QueryBuilders.matchQuery("name", "如家")
                        )
                        .mustNot(
                                QueryBuilders.rangeQuery("price")
                                        .gt(300)
                        )
        );
        SearchResponse searchResponse = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }
    }

    @Test
    void testMatch4() throws IOException {
        SearchRequest request = new SearchRequest(HOTEL);
        request.source()
                .query(
                        QueryBuilders.boolQuery()
                                .must(
                                        QueryBuilders.matchQuery("name", "如家")
                                )
                                .mustNot(
                                        QueryBuilders.rangeQuery("price")
                                                .gt(300)
                                )
                )
                .sort("price", SortOrder.ASC)
                .sort("score", SortOrder.DESC);
        SearchResponse searchResponse = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }
    }

    @Test
    void name() throws IOException {
        //分页查询
        SearchRequest searchRequest = new SearchRequest(HOTEL);
        int from = (2 - 1) * 10;

        searchRequest.source()
                .query(
                        QueryBuilders.matchQuery("name", "如家")
                )
                .from(from)
                .size(10);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }
    }

    @Test
    void hightLighterTest() throws IOException {

        SearchRequest searchRequest = new SearchRequest(HOTEL);
        searchRequest.source()
                .query(
                        QueryBuilders.matchQuery("all", "如家")
                )
                .highlighter(
                        new HighlightBuilder()
                                .field("all")
                                .preTags("<span style='color: red;'>")
                                .postTags("</span>")
                );
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField all = highlightFields.get("all");
            if (Objects.nonNull(all)) {
                Text[] fragments = all.fragments();
                for (Text fragment : fragments) {
                    System.out.println("fragment.string() = " + fragment.string());
                }
            }
        }

    }
}
