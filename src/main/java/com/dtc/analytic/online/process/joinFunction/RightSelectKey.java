package com.dtc.analytic.online.process.joinFunction;

import org.apache.flink.api.java.functions.KeySelector;

public class RightSelectKey implements KeySelector<String, String> {
    @Override
    public String getKey(String s) throws Exception {
        return s;
    }
}
