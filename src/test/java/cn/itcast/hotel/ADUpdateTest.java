package cn.itcast.hotel;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * @author liudo
 * @version 1.0
 * @project hotel-demo
 * @description 广告更新测试
 * @date 2023/8/20 16:43:31
 */
@SpringBootTest
public class ADUpdateTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testUpdateAD() throws IOException {
        String[] ids = new String[]{
                "38609",
                "38665",
                "38812",
                "39106",
                "39141"
        };

        for (String id : ids) {
            UpdateRequest request = new UpdateRequest("hotel", id);
            request.doc("isAD", true);
            restHighLevelClient.update(request, RequestOptions.DEFAULT);
        }

    }
}
