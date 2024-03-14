package cn.itcast.hotel;


import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HotelDemoApplicationTests {
    public static final String HOTEL = "hotel";
    public static final String SOURCE = "{\n" +
            "  \"mappings\": {\n" +
            "    \"properties\": {\n" +
            "      \"id\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"name\": {\n" +
            "        \"type\": \"text\"\n" +
            "        , \"analyzer\": \"ik_max_word\"\n" +
            "        ,\"copy_to\": \"all\"\n" +
            "      },\n" +
            "      \"address\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"price\": {\n" +
            "        \"type\": \"integer\"\n" +
            "      },\n" +
            "      \"score\": {\n" +
            "        \"type\": \"integer\"\n" +
            "      },\n" +
            "      \"brand\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"city\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"starName\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"business\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"location\": {\n" +
            "        \"type\": \"geo_point\"\n" +
            "      },\n" +
            "      \"pic\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"all\": {\n" +
            "        \"type\": \"text\"\n" +
            "        , \"analyzer\": \"ik_max_word\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private HotelMapper hotelMapper;

    @Test
    void testCreate() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest(HOTEL);
        request.source(SOURCE, XContentType.JSON);
        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(HOTEL);
        restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDocCreate() throws IOException {
        Hotel hotel = hotelMapper.selectById(36934L);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        IndexRequest request = new IndexRequest(HOTEL).id("36934");
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        restHighLevelClient.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void tetGetDoc() throws IOException {
        GetRequest request = new GetRequest(HOTEL, "36934");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println("response = " + response);
    }

    @Test
    void testDocUpdate() throws IOException {
        UpdateRequest request = new UpdateRequest(HOTEL, "36934");
        /*request.doc("{\n" +
                "  \"name\": \"8天就地点\"\n" +
                "}", XContentType.JSON);*/


      /*  HotelDoc hotelDoc = new HotelDoc();
        hotelDoc.setName("九天酒店");
        request.doc(JSON.toJSONString(hotelDoc), XContentType.JSON);*/
        request.doc("name", "10天酒店", "brand", "ririririr");
        restHighLevelClient.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest(HOTEL, "36934");
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkDoc() throws IOException {
        BulkRequest request = new BulkRequest();
        List<Hotel> hotels = hotelMapper.selectList(null);
        hotels.stream().map(HotelDoc::new)
                .forEach(hotelDoc -> {
                    IndexRequest indexRequest = new IndexRequest(HOTEL).id(hotelDoc.getId().toString());
                    indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
                    request.add(indexRequest);
                });
        restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkDoc2() throws IOException {
       // StopWatch stopWatch = new StopWatch();
        //stopWatch.start();
        List<Hotel> hotels = hotelMapper.selectList(null);
        int totalHotels = hotels.size();
        int pageSize = 10;

        for (int i = 0; i < totalHotels; i += pageSize) {
            BulkRequest request = new BulkRequest();
            int endIndex = Math.min(i + pageSize, totalHotels);

            List<Hotel> hotelsBatch = hotels.subList(i, endIndex);
            hotelsBatch.stream().map(HotelDoc::new)
                    .forEach(hotelDoc -> {
                        IndexRequest indexRequest = new IndexRequest(HOTEL).id(hotelDoc.getId().toString());
                        indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
                        request.add(indexRequest);
                    });

            restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
          // stopWatch.stop();
            //System.out.println(stopWatch.totalTime());

        }
    }
    @Test
    void testBulkDoc3() throws IOException {
        int pageNum = 1;
        int pageSize = 10;

        while (true) {
            // Create Page object
            Page<Hotel> page = new Page<>(pageNum, pageSize);

            // Fetch the hotels
            IPage<Hotel> hotelPage = hotelMapper.selectPage(page, null);

            // Process the hotels
            BulkRequest request = new BulkRequest();
            hotelPage.getRecords().stream().map(HotelDoc::new)
                    .forEach(hotelDoc -> {
                        IndexRequest indexRequest = new IndexRequest(HOTEL).id(hotelDoc.getId().toString());
                        indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
                        request.add(indexRequest);
                    });
            restHighLevelClient.bulk(request, RequestOptions.DEFAULT);

            // If there are no more pages, break the loop
            if (!((Page<Hotel>)hotelPage).hasNext()) {
                break;
            }

            // Otherwise, move to the next page
            pageNum++;
        }
    }
    @Test
    void testBulkDoc4() throws IOException {
        int pageNum = 1;
        int pageSize = 1000; // Increase the page size

        // Create a BulkProcessor
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                        (request, bulkListener) -> restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                        new BulkProcessor.Listener() {
                            @Override
                            public void beforeBulk(long executionId, BulkRequest request) {}

                            @Override
                            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

                            @Override
                            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {}
                        })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .build();

        while (true) {
            IPage<Hotel> hotelPage = fetchHotels(pageNum, pageSize);
            addHotelsToBulkProcessor(hotelPage.getRecords(), bulkProcessor);

            if (!hasNextPage(hotelPage)) {
                break;
            }

            pageNum++;
        }

        // Close the BulkProcessor
        bulkProcessor.close();
    }

    private IPage<Hotel> fetchHotels(int pageNum, int pageSize) {
        Page<Hotel> page = new Page<>(pageNum, pageSize);
        return hotelMapper.selectPage(page, null);
    }

    private void addHotelsToBulkProcessor(List<Hotel> hotels, BulkProcessor bulkProcessor) {
        hotels.stream().map(HotelDoc::new)
                .forEach(hotelDoc -> {
                    IndexRequest indexRequest = new IndexRequest(HOTEL).id(hotelDoc.getId().toString());
                    indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
                    bulkProcessor.add(indexRequest);
                });
    }


    private boolean hasNextPage(IPage<Hotel> hotelPage) {
        return ((Page<Hotel>)hotelPage).hasNext();
    }
    private final static ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(
            17,
            50,
            10,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(50)
    );
    @Test
    void test() throws InterruptedException {
        // 时间监听
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Integer count = hotelMapper.selectCount(null);
        int pageSize = 10;
        int page = (count / pageSize);
        if (count % pageSize > 0) {
            page++;
        }
        // 10 9 8 7 6 5 4 3 2 1 0
        CountDownLatch countDownLatch = new CountDownLatch(page);
        for (int i = 0; i < page; i++) {
            int finalI = i;
            EXECUTOR.execute(() -> {
                try {
                    Page<Hotel> hotelPage = hotelMapper.selectPage(
                            new Page<>(finalI, pageSize, false),
                            null);
                    BulkRequest request = new BulkRequest();
                    // 查询数据库所有信息
                    List<Hotel> hotels = hotelPage.getRecords();
                    hotels.stream()
                            .map(HotelDoc::new)
                            .forEach(hotelDoc -> {
                                // 构建单个请求
                                IndexRequest indexRequest = new IndexRequest(HOTEL).id(hotelDoc.getId().toString());
                                indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
                                // 构建批次请求
                                request.add(indexRequest);
                            });

                    restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        // 0 阻塞
        countDownLatch.await();
        stopWatch.stop();
        System.out.println("执行任务时间为:" + stopWatch.getTotalTimeMillis() + "毫秒");

    }

}
