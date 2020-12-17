package sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.ArrayList;
import java.util.HashMap;

//todo 将数据写入ES
public class ToES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDs = env.socketTextStream("hadoop102", 9999);
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        //builder需要的集合
        httpHosts.add(new HttpHost("hadoop102",9200));
        //1配置builder
        ElasticsearchSink.Builder<String> ESBuilder = new ElasticsearchSink.Builder<>(httpHosts, new MyESFunc());

        //2.一下两个配置只能分开配置
        //2.1.设置每次刷写条数
        ESBuilder.setBulkFlushMaxActions(1);
        //2.2 build
        ElasticsearchSink<String> ESbuild = ESBuilder.build();
        socketDs.addSink(ESbuild);
        //3.执行
        env.execute();
    }


    private static class MyESFunc implements ElasticsearchSinkFunction<String> {
        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            //对输入数据处理
            String[] split = element.split(",");

            //创建map存放字段
            HashMap<String, String> map = new HashMap<>();
            map.put("id",split[0]);
            map.put("ts",split[1]);
            map.put("temp",split[2]);
            System.out.println(map);

            //创建indexRequest
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("doc")
                    .id(split[0]+"es")
                    .source(map);
            //写入ES
            indexer.add(indexRequest);
        }
    }
}

