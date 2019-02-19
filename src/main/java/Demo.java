import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Demo {

    private TransportClient client;
    private IndexResponse indexResponse;
    private GetResponse getResponse;
    private DeleteResponse deleteResponse;

    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "docker-cluster").build();
        client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("10.40.40.41"), 9300));
    }

    @Test
    public void createIndex() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "lucy");
        json.put("postDate", new Date());
        json.put("message", "trying out es");
        indexResponse = client.prepareIndex("person", "doc", "1").setSource(json).get();
        RestStatus status = indexResponse.status();
        System.out.println(status);
        System.out.println(status.getStatus());
        System.out.println();
    }

    @Test
    public void batCreateIndex() throws IOException {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "jack");
        json.put("postDate", new Date());
        json.put("message", "trying out jack");

        Map<String, Object> json2 = new HashMap<String, Object>();
        json2.put("user", "tom");
        json2.put("postDate", new Date());
        json2.put("message", "trying out tom");

        bulkRequest.add(client.prepareIndex("person", "doc", "2").setSource(json));
        bulkRequest.add(client.prepareIndex("person", "doc", "3").setSource(json2));

        BulkResponse bulkResponse = bulkRequest.get();//执行插入操作
        System.out.println();
        if (bulkResponse.hasFailures()) {
            System.out.println("-------------------我失败了-----------------");
        }
    }

    @Test
    public void get1() throws UnknownHostException {
        getResponse = client.prepareGet("person", "doc", "1").get();
        Map<String, Object> source = getResponse.getSource();
        if (source != null) {
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                System.out.println(entry.getKey() + "--->" + entry.getValue());
            }
        }
        System.out.println();
    }

    @Test
    public void get2() throws UnknownHostException {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("person", "doc", "1").add("person", "doc", "2", "3").get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
        System.out.println();
    }

    /**
     * 根据文档进行删除
     *
     * @throws UnknownHostException
     */
    @Test
    public void delete1() throws UnknownHostException {
        deleteResponse = client.prepareDelete("person", "doc", "1").get();
        RestStatus status = deleteResponse.status();
        System.out.println(status);
        System.out.println(status.getStatus());
        System.out.println();
    }

    @After
    public void close() throws UnknownHostException {
        if (indexResponse != null) {
            client.close();
        }
        if (getResponse != null) {
            client.close();
        }
        if (deleteResponse != null) {
            client.close();
        }
    }
}