package com.dtc.analytic.online.process.joinFunction;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LeftJoinResult implements CoGroupFunction<String,String, Tuple2<String,String>> {
    @Override
    public void coGroup(Iterable<String> leftElements, Iterable<String> rightElements, Collector<Tuple2<String, String>> collector) throws Exception {
        for (String leftElement : leftElements) {
            boolean hadElement = false;
            for (String rightElement : rightElements) {
                collector.collect(Tuple2.of(leftElement,rightElement));
                hadElement = true;
                break;
            }
            if (!hadElement) {
                collector.collect(Tuple2.of(leftElement,null));
            }
        }
    }
}
