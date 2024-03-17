package cn.itcast.hotel;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@Slf4j
@SpringBootTest
public class SuggestionTest {
    public static final String HOTEL = "hotel";
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testCase01() throws IOException {

        SearchRequest searchRequest = new SearchRequest(HOTEL);
        searchRequest.source()
                .suggest(
                        new SuggestBuilder()
                                .addSuggestion("suggestionSugg"
                                        , SuggestBuilders.completionSuggestion("suggestion")
                                                .prefix("h")
                                                .skipDuplicates(true)
                                                .size(10)
                                )
                );

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Suggest suggest = searchResponse.getSuggest();
        CompletionSuggestion suggestionSuggest = suggest.getSuggestion("suggestionSugg");
        suggestionSuggest.getOptions().stream()
                .map(option -> option.getText().string())
                .forEach(item-> System.out.println("item"+item));
    }
}
