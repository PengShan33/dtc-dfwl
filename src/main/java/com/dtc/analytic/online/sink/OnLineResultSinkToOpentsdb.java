package com.dtc.analytic.online.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.Metric;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;

public class OnLineResultSinkToOpentsdb extends RichSinkFunction<Tuple2<String, String>> {
    String properties;
    HttpClientImpl httpClient;

    public OnLineResultSinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = new HttpClientImpl(properties);
    }

    @Override
    public void invoke(Tuple2<String, String> value) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        String deviceId = value.f0;
        // isOnLine: 0 离线;1 在线
        int isOnLine = 0;
        long curTime = System.currentTimeMillis() / 1000;
        if (value.f1 != null) {
            isOnLine = 1;
        }

        String metric = deviceId + "_" + "onLineCondition";
        builder.addMetric(metric)
                .setDataPoint(curTime,isOnLine)
                .addTag("host",deviceId)
                .addTag("IndexType","onLineCondition");
        Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
        boolean success = response.isSuccess();
    }
}
