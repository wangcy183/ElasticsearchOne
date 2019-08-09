package com.dfjinxin.elasticsearch.ElasticCURD;

import com.dfjinxin.elasticsearch.config.ElasticConfig;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
public class ElasticCRUD {
    @Autowired
    private ElasticConfig elasticConfig;
    //通过id查询文档
    @GetMapping("/get/item/docs")
    @ResponseBody
    public ResponseEntity get(@RequestParam(name="id",defaultValue = "") String id) throws UnknownHostException {

        if(id.isEmpty()){
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        GetResponse res=this.elasticConfig.transportClient().prepareGet("item","docs",id).get();
        if(!res.isExists()){
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(res.getSource(),HttpStatus.OK);
    }
    //增加接口
    @PostMapping("/post/item/docs")
    @ResponseBody
    public ResponseEntity addDocs(
            @RequestParam(name="title") String title,
            @RequestParam(name="category") String category,
            @RequestParam(name="brand") String brand,
            @RequestParam(name="price") Integer price,
            @RequestParam(name="images") String images
    ){

        try {
            XContentBuilder content=XContentFactory
                    .jsonBuilder()
                    .startObject()
                    .field("title",title)
                    .field("category",category)
                    .field("brand",brand)
                    .field("price",price)
                    .field("images",images)
                    .endObject();
            IndexResponse res=this.elasticConfig.transportClient().prepareIndex("item","docs")
                    .setSource(content)
                    .get();
            return new ResponseEntity(res.getId(),HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    @DeleteMapping("/delete/docs")
    @ResponseBody
    public ResponseEntity deleteDocs(@RequestParam(name="id") String id){

        DeleteRequest request=new DeleteRequest().id(id);
        try {
            DeleteResponse res=this.elasticConfig.transportClient().prepareDelete("item","docs",id).get();
            return  new ResponseEntity(res.getId(),HttpStatus.OK);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return  new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    @PutMapping("/put/docs")
    @ResponseBody
    public ResponseEntity updateDocs(
            @RequestParam(name="id") String id,
            @RequestParam(name="title",required = false) String title,
            @RequestParam(name="category",required = false) String category,
            @RequestParam(name="brand",required = false) String brand,
            @RequestParam(name="price",required = false) Integer price,
            @RequestParam(name="images",required = false) String images
    ){
        UpdateRequest request=new UpdateRequest("item","docs",id);
        try {
            XContentBuilder contentBuilder=XContentFactory.jsonBuilder().startObject();
            if(title!=null){
                contentBuilder.field("title",title);
            }
            if(category!=null){
                contentBuilder.field("category",category);
            }
            if(brand!=null){
                contentBuilder.field("brand",brand);
            }
            if(price!=null){
                contentBuilder.field("price",price);
            }
            if(images!=null){
                contentBuilder.field("images",images);
            }
            contentBuilder.endObject();
            request.doc(contentBuilder);
        } catch (IOException e) {
            e.printStackTrace();
            return  new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        try {
            UpdateResponse res= this.elasticConfig.transportClient().update(request).get();
            return new ResponseEntity(res.getId(),HttpStatus.OK);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    //复合查询
    @PostMapping("/query/item/docs")
    @ResponseBody
    public ResponseEntity selectDocs(
            @RequestParam(name="title",required = false) String title,
            @RequestParam(name="price",required = false) Integer price,
            @RequestParam(name="gt_price",defaultValue = "0") Integer gtPrice,
            @RequestParam(name="lt_price",required = false) Integer ltPrice
    ){
        BoolQueryBuilder booleanQuery= QueryBuilders.boolQuery();
        if(title!=null){
            booleanQuery.must(QueryBuilders.matchQuery("title",title));
        }
        RangeQueryBuilder rangeQueryBuilder=QueryBuilders.rangeQuery("price").from(gtPrice);
        if(ltPrice!=null && ltPrice>0){
            rangeQueryBuilder.to(ltPrice);
        }
        booleanQuery.filter(rangeQueryBuilder);
        SearchRequestBuilder searchRequestBuilder= null;
        try {
            searchRequestBuilder = this.elasticConfig.transportClient().prepareSearch("item")
                    .setTypes("docs")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(booleanQuery)
                    .setFrom(0)
                    .setSize(10);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        SearchResponse res=searchRequestBuilder.get();
        List<Map<String,Object>> result=new ArrayList<>();
        for(SearchHit hit:res.getHits()){
            result.add(hit.getSourceAsMap());
        }
        return new ResponseEntity(result,HttpStatus.OK);
    }
}
