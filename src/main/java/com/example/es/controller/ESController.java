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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import javax.naming.directory.SearchResult;
import java.io.IOException;
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
        request.id("1");
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
    @GetMapping(value = "/es/document/info/{index}/{id}")
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
                    .id((i + 1) + "")
                    .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }
        BulkResponse response =  highLevelClient.bulk(request,RequestOptions.DEFAULT);
        return !response.hasFailures();
    }

    /**
     * 精确查询/匹配所有查询 - 文档
     * @param index
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/es/document/search/{index}")
    public String searchDocument(@PathVariable(value = "index") String index) throws IOException {
        SearchRequest result  = new SearchRequest(index);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.highlighter();
        sourceBuilder.timeout(TimeValue.timeValueSeconds(1));
        /**
         * 精确查询/匹配所有
         */
        TermQueryBuilder termQueryBuilder =  QueryBuilders.termQuery("mame","just");
//        MatchAllQueryBuilder matchAllQueryBuilder1 = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);

        result.source(sourceBuilder);
        SearchResponse searchResponse = highLevelClient.search(result, RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            logger.info(hit.getSourceAsMap().toString());
        }
        return "success";
    }



}
