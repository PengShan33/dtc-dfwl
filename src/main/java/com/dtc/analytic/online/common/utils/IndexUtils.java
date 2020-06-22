package com.dtc.analytic.online.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.util.Collector;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.request.QueryBuilder;
import org.opentsdb.client.request.SubQueries;
import org.opentsdb.client.response.SimpleHttpResponse;
import org.opentsdb.client.util.Aggregator;

import java.io.IOException;
import java.util.*;

public class IndexUtils {
    private static String opentsdb_url;
    private static HttpClientImpl httpClient;

    public static DataStream<Tuple4<String, Double, Double, Double>> indexCalculate(DataStreamSource<String> inputStream, String prop) {
        opentsdb_url = prop;
        DataStream<Tuple4<String, Double, Double, Double>> resultStream = inputStream.flatMap(new IndexCalculate());
        return resultStream;
    }

    /*private static class IndexCalculate implements FlatMapFunction<String,Tuple4<String,Double,Double,Double>> {
        @Override
        public void flatMap(String deviceId, Collector<Tuple4<String, Double, Double, Double>> collector) throws Exception {
            Tuple4<String, Double, Double, Double> indexResult = getIndexResult(deviceId);
            collector.collect(indexResult);
        }
    }*/

    private static class IndexCalculate extends RichFlatMapFunction<String, Tuple4<String, Double, Double, Double>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            httpClient = new HttpClientImpl(opentsdb_url);
        }

        @Override
        public void flatMap(String deviceId, Collector<Tuple4<String, Double, Double, Double>> collector) throws Exception {
            Tuple4<String, Double, Double, Double> indexResult = getIndexResult(deviceId);
            collector.collect(indexResult);
        }
    }

    private static Tuple4<String, Double, Double, Double> getIndexResult(String deviceId) {
        Tuple4<String, Double, Double, Double> result = new Tuple4<>();

        QueryBuilder builder = QueryBuilder.getInstance();
        SubQueries subQueries = new SubQueries();
        String zimsum = Aggregator.zimsum.toString();

        String metric = deviceId + "_" + "onLineCondition";
        subQueries.addMetric(metric).addAggregator(zimsum);
        long startTime = getStartTime();
        long endTime = getEndTime();
        builder.getQuery().addStart(startTime).addEnd(endTime).addSubQuery(subQueries);

        try {
            SimpleHttpResponse response = httpClient.pushQueries(builder, ExpectResponse.STATUS_CODE);
            String content = response.getContent();
            int statusCode = response.getStatusCode();
            Map<Object, Integer> seqMap = new LinkedHashMap<Object, Integer>();

            if (statusCode == 200) {
                JSONArray jsonArray = JSON.parseArray(content);
                for (Object object : jsonArray) {
                    JSONObject json = (JSONObject) JSON.toJSON(object);
                    String dps = json.getString("dps");
                    Map<String, String> map = JSON.parseObject(dps, Map.class);
                    Set<String> strings = map.keySet();
                    Object[] objects = strings.toArray();
                    Arrays.sort(objects);
                    int i = 0;
                    for (Object entry : objects) {
//                        System.out.println(i + ": " + "Time:" + entry + ",Value:" + Integer.parseInt(String.valueOf(map.get(entry))));
                        seqMap.put(i, Integer.parseInt(String.valueOf(map.get(entry))));
                        i++;
                    }
                }
            }

            // 离线计数
            double a = 0;
            // 在线计数
            double b = 0;
            for (int i = 0; i < seqMap.keySet().size() - 1; i++) {
                Integer integer = seqMap.get(i);
                if (integer == 0) {
                    a++;
                } else {
                    b++;
                }
            }
            double offLineTime = a * 5;
            double onLineTime = b * 5;
            double onLineRate = b / (a + b);

            result = Tuple4.of(deviceId, onLineTime, offLineTime, onLineRate);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static long getStartTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        long startTime = calendar.getTime().getTime() / 1000;
        return startTime;
    }

    private static long getEndTime() {
        long endTime = System.currentTimeMillis() / 1000;
        return endTime;
    }
}
