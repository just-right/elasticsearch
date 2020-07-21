package com.example.es.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.es.entity.SearchResultInfo;
import com.example.es.entity.Shop;
import com.example.es.entity.ShopSearchInfo;
import com.example.es.entity.User;
import com.google.gson.Gson;
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
import org.springframework.data.elasticsearch.core.completion.Completion;
import org.springframework.web.bind.annotation.*;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class ESController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient highLevelClient;

    @PostMapping(value = "/es/document/create/shop")
    public String createShopDocument(@RequestBody Shop shop) throws IOException {
        IndexRequest indexRequest = new IndexRequest("shop");
        indexRequest.timeout(TimeValue.timeValueSeconds(1));
        shop.setSuggest(new Completion(shop.getTags()));
        /**
         * 使用Gson --
         * GeoPoint传入时传经纬度 --- 使用fastjson会解析错误
         * by https://agentd.cn/archives/es-geopoint
         */
        indexRequest.source(new Gson().toJson(shop), XContentType.JSON);
        IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.toString();
    }


    @PostMapping(value = "/es/document/search/shop")
    public String shopSearch(@RequestBody ShopSearchInfo shopSearchInfo) throws IOException {
        SearchRequest searchRequest = new SearchRequest("shop");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        /**
         * 搜索建议
         */
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders
                .completionSuggestion(Shop.SUGGEST)
                .prefix(shopSearchInfo.getShopName());
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        //自定义搜索名
        suggestBuilder.addSuggestion("shopSearch", suggestionBuilder);

        /**
         * GEO位置搜索
         */
        GeoPoint geoPoint = new GeoPoint(shopSearchInfo.getLatitude(), shopSearchInfo.getLongitude());
        //geo距离查询
        QueryBuilder queryBuilder = QueryBuilders.geoDistanceQuery(Shop.LOCATION)
                .distance(shopSearchInfo.getLen(), DistanceUnit.KILOMETERS)
                .point(geoPoint);

        sourceBuilder.suggest(suggestBuilder);
        sourceBuilder.query(queryBuilder);

        searchRequest.source(sourceBuilder);

        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);


        List<String> geoResList = new ArrayList<>();
        SearchHit[] searchHits = response.getHits().getHits();
        if (searchHits == null || searchHits.length <= 0) {
            return "未搜索到相关信息！";
        }
        for (SearchHit hit : searchHits) {
            geoResList.add(hit.getId());
        }
        CompletionSuggestion suggestion = response.getSuggest().getSuggestion("shopSearch");
        List<CompletionSuggestion.Entry.Option> optionList = suggestion.getOptions();
        if (optionList == null || optionList.size() <= 0) {
            return "未搜索到相关信息！";
        }
        List<String> suggestResList = new ArrayList<>();
        for (CompletionSuggestion.Entry.Option option : optionList) {
            suggestResList.add(option.getHit().getId());
        }
        /**
         * 寻找相同元素
         */
        geoResList.retainAll(suggestResList);
        List<SearchResultInfo> resList = new ArrayList<>();
        for (String shopID : geoResList) {
            GetRequest request = new GetRequest("shop", shopID);
            GetResponse searchResponse = highLevelClient.get(request, RequestOptions.DEFAULT);
            Map<String, Object> resultMap = searchResponse.getSourceAsMap();
            if (resultMap == null || resultMap.size() <= 0) {
                continue;
            }
            String shopName = (String) resultMap.get(Shop.SHOPNAME);
            Map<String, Double> geoInfo = (Map<String, Double>) resultMap.get(Shop.LOCATION);
            double _lon1 = geoInfo.get("lon");
            double _lat1 = geoInfo.get("lat");
            double _lon2 = shopSearchInfo.getLongitude();
            double _lat2 = shopSearchInfo.getLatitude();
            double len = this.getDistance(_lat1, _lon1, _lat2, _lon2);
            SearchResultInfo resultInfo = new SearchResultInfo();
            resultInfo.setShopID(shopID).setShopName(shopName).setLen(len);
            resList.add(resultInfo);
        }
        /**
         * 按照距离排序 - 倒序
         */
        resList = resList.stream().distinct().sorted(Comparator
                .comparing(SearchResultInfo::getLen)).collect(Collectors.toList());
        return JSONObject.toJSONString(resList);
    }

    public double getDistance(double _lat1, double _lon1, double _lat2, double _lon2) {
        double lat1 = (Math.PI / 180) * _lat1;
        double lat2 = (Math.PI / 180) * _lat2;
        double lon1 = (Math.PI / 180) * _lon1;
        double lon2 = (Math.PI / 180) * _lon2;
        //地球半径
        double R = 6378.1;
        double d = Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1)) * R;
        return new BigDecimal(d).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 获取文档信息
     *
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
     *
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
     *
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
     * 更新文档信息
     *
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
     *
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
     *
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
        BulkResponse response = highLevelClient.bulk(request, RequestOptions.DEFAULT);
        return !response.hasFailures();
    }

    /**
     * 精确查询/匹配所有查询 - 文档 -高亮显示
     *
     * @param index
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/search/{index}")
    public String searchDocument(@PathVariable(value = "index") String index) throws IOException {
        SearchRequest result = new SearchRequest(index);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.timeout(TimeValue.timeValueSeconds(1));
        /**
         * 精确查询/匹配所有
         */
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "just");
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
     *
     * @param index
     * @param suggest
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/search/{index}/{suggest}")
    public List<String> searchDocument(@PathVariable(value = "index") String index, @PathVariable(value = "suggest") String suggest) throws IOException {
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
        suggest = URLDecoder.decode(suggest, "utf-8");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders
                .completionSuggestion("suggest")
                .prefix(suggest);

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        //自定义搜索名
        suggestBuilder.addSuggestion("s-test", suggestionBuilder);

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
        GeoPoint geoPoint = new GeoPoint(39.93367367974064d, 116.47845257733152d);
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
