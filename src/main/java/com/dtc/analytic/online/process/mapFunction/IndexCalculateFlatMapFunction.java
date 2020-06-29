package com.dtc.analytic.online.process.mapFunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtc.analytic.online.common.constant.PropertiesConstants;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.request.QueryBuilder;
import org.opentsdb.client.request.SubQueries;
import org.opentsdb.client.response.SimpleHttpResponse;
import org.opentsdb.client.util.Aggregator;

import java.io.IOException;
import java.util.*;

public class IndexCalculateFlatMapFunction extends RichFlatMapFunction<String, Tuple5<String, Double, Double, Double, Double>> {
    private HttpClientImpl httpClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String opentsdb_url = parameters.getString(PropertiesConstants.OPENTSDB_URL, "http://10.3.7.232:4399");
        httpClient = new HttpClientImpl(opentsdb_url);
    }

    @Override
    public void flatMap(String deviceId, Collector<Tuple5<String, Double, Double, Double, Double>> collector) throws Exception {
        Tuple5<String, Double, Double, Double,  Double> result = getResult(deviceId);
        collector.collect(result);
    }

    private Tuple5<String, Double, Double, Double,  Double> getResult(String deviceId) {
        Tuple5<String, Double, Double, Double,  Double> result = new Tuple5<>();

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
            // 连续离线计数
            double c = 0;
            for (int i = 0; i < seqMap.keySet().size() - 1; i++) {
                Integer integer = seqMap.get(i);
                if (integer == 0) {
                    c++;
                } else {
                    break;
                }
            }

            double offLineTime = a * 5;
            double onLineTime = b * 5;
            double offLineTime_c = c * 5;
            double onLineRate = b / (a + b);

            result = Tuple5.of(deviceId, onLineTime, offLineTime, offLineTime_c, onLineRate);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private long getEndTime() {
        long endTime = System.currentTimeMillis() / 1000;
        return endTime;
    }

    private long getStartTime() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        long startTime = calendar.getTime().getTime() / 1000;
        return startTime;
    }
}
