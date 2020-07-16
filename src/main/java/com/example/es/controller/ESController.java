package com.example.es.controller;

import com.alibaba.fastjson.JSON;
import com.example.es.entity.User;
import lombok.val;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

@RestController
public class ESController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient highLevelClient;

    /**
     * 创建索引
     *
     * @param name
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/create/index/{name}")
    public String createIndex(@PathVariable(value = "name") String name) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(name);
        CreateIndexResponse response = highLevelClient.indices().create(request, RequestOptions.DEFAULT);
        logger.info(response.toString());
        return "success";
    }

    /**
     * 判断索引是否存在
     *
     * @param name
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/index/exist/{name}")
    public boolean judgetIndexExist(@PathVariable(value = "name") String name) throws IOException {
        GetIndexRequest request = new GetIndexRequest(name);
        boolean isExisted = highLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        return isExisted;
    }

    /**
     * 删除索引
     *
     * @param name
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/index/del/{name}")
    public boolean indexDel(@PathVariable(value = "name") String name) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(name);
        AcknowledgedResponse response = highLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    /**
     * 添加文档
     * @param name
     * @param user
     * @return
     * @throws IOException
     */
    @PostMapping(value = "/es/document/add/{name}")
    public String documentAdd(@PathVariable(value = "name") String name, @RequestBody User user) throws IOException {
        IndexRequest request = new IndexRequest(name);
//        request.id("1"); id自动生成
        request.timeout(TimeValue.timeValueSeconds(1));
        request.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse response = highLevelClient.index(request, RequestOptions.DEFAULT);
        return "success";
    }

    /**
     * 判断文档是否存在
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/exist/{index}/{id}")
    public boolean documentExist(@PathVariable(value = "index") String index, @PathVariable(value = "id") String id) throws IOException {
        GetRequest request = new GetRequest(index, id);
        //不获取返回的_source的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        return highLevelClient.exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 获取文档信息
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/info/{index}/{id}")
    public String getDocument(@PathVariable(value = "index") String index, @PathVariable(value = "id") String id) throws IOException {
        GetRequest request = new GetRequest(index, id);
        GetResponse response = highLevelClient.get(request, RequestOptions.DEFAULT);
        return response.getSourceAsString();
    }

    /**
     * 更新文档信息
     * @param name
     * @param id
     * @param user
     * @return
     * @throws IOException
     */
    @PostMapping(value = "/es/document/update/{name}/{id}")
    public String documentUpdate(@PathVariable(value = "name") String name, @PathVariable(value = "id") String id, @RequestBody User user) throws IOException {
        UpdateRequest request = new UpdateRequest(name, id);
        request.timeout(TimeValue.timeValueSeconds(1));
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse response = highLevelClient.update(request, RequestOptions.DEFAULT);
        return "success";
    }

    /**
     * 删除文档
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/del/{index}/{id}")
    public String delDocument(@PathVariable(value = "index") String index, @PathVariable(value = "id") String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, id);
        request.timeout(TimeValue.timeValueMillis(1));
        DeleteResponse response = highLevelClient.delete(request, RequestOptions.DEFAULT);
        return "success";
    }

    /**
     * 批量新增文档
     * @param name
     * @param userList
     * @return
     * @throws IOException
     */
    @PostMapping(value = "/es/document/batchadd/{name}")
    public boolean documentBatchAdd(@PathVariable(value = "name") String name, @RequestBody List<User> userList) throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueSeconds(1));
        for (int i = 0; i < userList.size(); i++) {
            request.add(new IndexRequest(name)
//                    .id((i + 1) + "")
                    .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }
        BulkResponse response =  highLevelClient.bulk(request,RequestOptions.DEFAULT);
        return !response.hasFailures();
    }

    /**
     * 精确查询/匹配所有查询 - 文档 -高亮显示
     * @param index
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/search/{index}")
    public String searchDocument(@PathVariable(value = "index") String index) throws IOException {
        SearchRequest result  = new SearchRequest(index);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.timeout(TimeValue.timeValueSeconds(1));
        /**
         * 精确查询/匹配所有
         */
        TermQueryBuilder termQueryBuilder =  QueryBuilders.termQuery("name","just");
        MatchAllQueryBuilder matchAllQueryBuilder1 = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        /**
         * 高亮只匹配查询字段
         */
        highlightBuilder.field("name");
        highlightBuilder.preTags("<b>");
        highlightBuilder.postTags("</b>");
        sourceBuilder.highlighter(highlightBuilder);

        result.source(sourceBuilder);
        SearchResponse searchResponse = highLevelClient.search(result, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            logger.info(hit.getSourceAsString());
            HighlightField ageField = hit.getHighlightFields().get("name");
            for (Text fragment : ageField.getFragments()) {
                logger.info(fragment.toString());
            }
            logger.info(hit.getSourceAsMap().toString());
        }
        return "success";
    }


    /**
     * 搜索建议
     * @param index
     * @param suggest
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/search/{index}/{suggest}")
    public List<String> searchDocument(@PathVariable(value = "index") String index,@PathVariable(value = "suggest") String suggest) throws IOException {
        /**
         * 搜索建议 -- 新建索引
         * PUT /user
         * {
         *     "settings":{
         *         "analysis":{
         *             "analyzer":{
         *                 "default":{
         *                     "type":"ik_max_word"
         *                 }
         *             }
         *         }
         *     },
         *     "mappings":{
         *         "dynamic_date_formats": [
         *              "MM/dd/yyyy",
         *              "yyyy/MM/dd HH:mm:ss",
         *              "yyyy-MM-dd",
         *              "yyyy-MM-dd HH:mm:ss"
         *          ],
         *         "properties":{
         *             "suggest":{
         *                 "type":"completion"
         *             }
         *         }
         *     }
         * }
         *
         *
         * POST /user/_doc
         * {
         *     "name":"小王一号",
         *     "age":"18",
         *     "birthday":"2020-09-09",
         *     "suggest": {
         *        "input": "小王"
         *     }
         * }
         *
         * POST /user/_doc
         * {
         *     "name":"小王二号",
         *     "age":"18",
         *     "birthday":"2020-09-09",
         *     "suggest": {
         *        "input": "小王"
         *     }
         * }
         *
         * POST /user/_doc
         * {
         *     "name":"小周一号",
         *     "age":"18",
         *     "birthday":"2020-09-09",
         *     "suggest": {
         *        "input": "小周"
         *     }
         * }
         *
         */
        SearchRequest searchRequest = new SearchRequest(index);
        suggest = URLDecoder.decode(suggest,"utf-8");
//        suggest = "小王";
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders
                .completionSuggestion("suggest")
                .prefix(suggest);

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        //自定义搜索名
        suggestBuilder.addSuggestion("s-test",suggestionBuilder);

        sourceBuilder.suggest(suggestBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        CompletionSuggestion suggestion = response.getSuggest().getSuggestion("s-test");
        List<String> resList = new ArrayList<>();
        for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
            logger.info(option.getText().toString());
            resList.add(option.getHit().getSourceAsMap().get("name").toString());
        }
        return resList;
    }

    @GetMapping(value = "/es/document/search/geo/{index}")
    public List<String> geoSearchDocument(@PathVariable(value = "index") String index) throws IOException {
        /**
         * PUT /geo/
         * {
         *     "settings":{
         *         "analysis":{
         *             "analyzer":{
         *                 "default":{
         *                     "type":"ik_max_word"
         *                 }
         *             }
         *         }
         *     },
         *     "mappings":{
         *         "dynamic_date_formats":[
         *             "MM/dd/yyyy",
         *             "yyyy/MM/dd HH:mm:ss",
         *             "yyyy-MM-dd",
         *             "yyyy-MM-dd HH:mm:ss"
         *         ],
         *         "properties":{
         *             "location":{
         *                 "type":"geo_point"
         *             }
         *         }
         *     }
         * }
         *
         * ---- 北京站
         * POST /geo/_doc
         * {
         *     "name":"路人甲",
         *     "location":{
         *         "lat": 39.90279998006104,
         *         "lon": 116.42703999493406
         *     }
         * }
         * ---- 朝阳公园
         * POST /geo/_doc
         * {
         *     "name":"路人乙",
         *     "location":{
         *         "lat": 39.93367367974064,
         *         "lon": 116.47845257733152
         *     }
         * }
         */

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //工体的坐标
        GeoPoint geoPoint = new GeoPoint(39.93367367974064d,116.47845257733152d);
        //geo距离查询  name=geo字段
        QueryBuilder queryBuilder = QueryBuilders.geoDistanceQuery("location")
                //距离 3KM
                .distance(3d, DistanceUnit.KILOMETERS)
                //坐标工体
                .point(geoPoint);

        //把查询结果按照离“我”的距离排序
        GeoDistanceSortBuilder sortBuilder = SortBuilders
                .geoDistanceSort("location", geoPoint)
                .order(SortOrder.ASC);

        sourceBuilder.sort(sortBuilder);
//        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        List<String> resList = new ArrayList<>();

        for (SearchHit hit : response.getHits().getHits()) {
            resList.add(hit.getSourceAsMap().get("name").toString());
        }
        return resList;
    }





}
