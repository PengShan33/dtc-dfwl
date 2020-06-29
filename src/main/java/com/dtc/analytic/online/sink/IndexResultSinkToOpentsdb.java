package com.dtc.analytic.online.sink;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.opentsdb.client.ExpectResponse;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.builder.MetricBuilder;
import org.opentsdb.client.response.Response;

public class IndexResultSinkToOpentsdb extends RichSinkFunction<Tuple5<String, Double, Double, Double, Double>> {
    String properties;
    HttpClientImpl httpClient;

    public IndexResultSinkToOpentsdb(String prop) {
        this.properties = prop;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = new HttpClientImpl(properties);
    }

    @Override
    public void invoke(Tuple5<String, Double, Double, Double, Double> value) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        long curTime = System.currentTimeMillis() / 1000;
        String deviceId = value.f0;
        String metric01 = deviceId + "_" + "onLineDuration";
        String metric02 = deviceId + "_" + "offLineDuration";
        String metric03 = deviceId + "_" + "onLineRate";

        builder.addMetric(metric01)
                .setDataPoint(curTime, value.f1)
                .addTag("host", deviceId)
                .addTag("IndexType", "onLineDuration");
        builder.addMetric(metric02)
                .setDataPoint(curTime, value.f2)
                .addTag("host", deviceId)
                .addTag("IndexType", "offLineDuration");
        builder.addMetric(metric03)
                .setDataPoint(curTime, value.f4)
                .addTag("host", deviceId)
                .addTag("IndexType", "onLineRate");


        Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
        boolean success = response.isSuccess();
    }

}
