package com.dtc.analytic.online.common.utils;

import com.dtc.analytic.online.common.modle.AlterStruct;
import com.dtc.analytic.online.common.modle.TimesConstants;
import com.dtc.analytic.online.process.processFunction.getAlarmProcessFunction;
import com.dtc.analytic.online.process.processFunction.getOffLineAlarmProcessFunction;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlarmUtils {
    public static List<DataStream<AlterStruct>> getAlarm(DataStream<Tuple5<String, Double, Double, Double, Double>> indexResult, BroadcastStream<Map<String, String>> broadcast, TimesConstants build) {
//        indexResult.print("test-dataflow:");
        SingleOutputStreamOperator<AlterStruct> alert_rule = indexResult.connect(broadcast).process(new getAlarmProcessFunction());
//        alert_rule.print("test:");

        // 收敛规则
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<AlterStruct, ?> alarmGrade =
                Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                        .where(new SimpleCondition<AlterStruct>() {
                            @Override
                            public boolean filter(AlterStruct s) {
                                return s.getLevel().equals("1");
                            }
                        }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("2");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("3");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("4");
                    }
                }).times(build.getOne()).within(Time.seconds(build.getTwo()));
        Pattern<AlterStruct, ?> alarmIncream
                = Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                .where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("1");
                    }
                }).next("middle").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("2");
                    }
                }).next("three").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("3");
                    }
                }).next("finally").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("4");
                    }
                }).times(build.getThree()).within(Time.seconds(build.getFour()));
        PatternStream<AlterStruct> patternStream =
                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmGrade);
        PatternStream<AlterStruct> alarmIncreamStream =
                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmIncream);
        DataStream<AlterStruct> alarmStream =
                patternStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(0);
                    }
                });
        DataStream<AlterStruct> alarmStreamIncream =
                alarmIncreamStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(2);
                    }
                });

        List<DataStream<AlterStruct>> list = new ArrayList<>();
        list.add(alarmStream);
        list.add(alarmStreamIncream);
        return list;
    }

    public static AlterStruct getAlarmResult(Tuple3<String, String, Double> value, ReadOnlyBroadcastState<String, String> broadcastState) throws Exception {
        String alarmInfo = broadcastState.get(value.f1).trim();
        // s8.142=856|142|113:s8:142:SXT08:哈楼:null|null|30.0|null:今日离线率
        String[] split = alarmInfo.split(":");
        if (split.length != 7) {
            return null;
        }
        String unique_id = split[0].trim();

        String r_code = split[2].trim();
        String asset_code = split[3].trim();
        String asset_name = split[4].trim();
        String result = asset_code + "(" + asset_name + ")";
        String r_value = split[5].trim();
        if (unique_id.isEmpty() || r_value.isEmpty()) {
            return null;
        }
        String[] split1 = r_value.split("\\|");
        if (split1.length != 4) {
            return null;
        }

        String[] IDS = unique_id.split("\\|");
        String asset_id = IDS[0];
        String indice_id = IDS[1];
        String strategy_id = IDS[2];
        String triger_name = split[6];
        String device_id = split[1];

        Tuple6<String, String, String, Integer, Double, Double> alarmResult = AlarmRule(value, device_id, split1);

        String curTime = alarmResult.f2;
        String level = alarmResult.f3.toString();
        String relValue = alarmResult.f4.toString();
        String yuzhi = alarmResult.f5.toString();
        String weiyi = value.f1;

        AlterStruct alterStruct = new AlterStruct(device_id, asset_name, triger_name, curTime, relValue, level, unique_id, yuzhi, weiyi);

        return alterStruct;
    }

    public static Tuple6<String, String, String, Integer, Double, Double> AlarmRule(Tuple3<String, String, Double> value, String device_id, String[] split1) {
        double data_value = value.f2;
        // device_id,index_code,time,level,value,yuzhi
        Tuple6<String, String, String, Integer, Double, Double> result = new Tuple6<>();
        if (device_id.equals(value.f0)) {
            String level_1 = split1[0];
            String level_2 = split1[1];
            String level_3 = split1[2];
            String level_4 = split1[3];

            //四个阈值都不为空
            if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4))) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_2 = Double.parseDouble(split1[1]);
                Double num_3 = Double.parseDouble(split1[2]);
                Double num_4 = Double.parseDouble(split1[3]);
                if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                } else if ((data_value > num_3 || data_value == num_3) && data_value < num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                } else if (data_value > num_4 || data_value == num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            }
            //一个不为空，其他的都为空
            else if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
                Double num_1 = Double.parseDouble(split1[0]);
                if ((data_value > num_1 || data_value == num_1)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                }
            } else if ("null".equals(level_1) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
                Double num_2 = Double.parseDouble(split1[1]);
                if ((data_value > num_2 || data_value == num_2)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                }
            } else if ("null".equals(level_1) && "null".equals(level_2) && !("null".equals(level_3)) && "null".equals(level_4)) {
                Double num_3 = Double.parseDouble(split1[2]);
                if ((data_value > num_3 || data_value == num_3)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                }
            } else if ("null".equals(level_1) && "null".equals(level_2) && "null".equals(level_3) && !("null".equals(level_4))) {
                Double num_4 = Double.parseDouble(split1[3]);
                if ((data_value > num_4 || data_value == num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            }
            //两个为空，两个不为空
            else if (!("null".equals(level_1)) && !("null".equals(level_2)) && "null".equals(level_3) && "null".equals(level_4)) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_2 = Double.parseDouble(split1[1]);
                if ((data_value > num_1 || data_value == num_1) && (data_value < num_2)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_2 || data_value == num_2)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_2);
                }
            } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && "null".equals(level_2) && "null".equals(level_4)) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_3 = Double.parseDouble(split1[2]);
                if ((data_value > num_1 || data_value == num_1) && (data_value < num_3)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_3 || data_value == num_3)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                }
            } else if (!("null".equals(level_1)) && !("null".equals(level_4)) && "null".equals(level_2) && "null".equals(level_3)) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_4 = Double.parseDouble(split1[3]);
                if ((data_value > num_1 || data_value == num_1) && (data_value < num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_4 || data_value == num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_1) && "null".equals(level_4)) {
                Double num_3 = Double.parseDouble(split1[2]);
                Double num_2 = Double.parseDouble(split1[1]);
                if ((data_value > num_2 || data_value == num_2) && (data_value < num_3)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                } else if ((data_value > num_3 || data_value == num_3)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                }
            } else if (!("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_3)) {
                Double num_4 = Double.parseDouble(split1[3]);
                Double num_2 = Double.parseDouble(split1[1]);
                if ((data_value > num_2 || data_value == num_2) && (data_value < num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                } else if ((data_value > num_4 || data_value == num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            } else if (!("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1) && "null".equals(level_2)) {
                Double num_4 = Double.parseDouble(split1[3]);
                Double num_3 = Double.parseDouble(split1[2]);
                if ((data_value > num_3 || data_value == num_3) && (data_value < num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                } else if ((data_value > num_4 || data_value == num_4)) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            }
            //三不空，一空
            else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_3)) && "null".equals(level_4)) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_2 = Double.parseDouble(split1[1]);
                Double num_3 = Double.parseDouble(split1[2]);
                if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                } else if (data_value > num_3 || data_value == num_3) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                }
            } else if (!("null".equals(level_1)) && !("null".equals(level_2)) && !("null".equals(level_4)) && "null".equals(level_3)) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_2 = Double.parseDouble(split1[1]);
                Double num_4 = Double.parseDouble(split1[3]);
                if ((data_value > num_1 || data_value == num_1) && data_value < num_2) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_2 || data_value == num_2) && data_value < num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                } else if (data_value > num_4 || data_value == num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            } else if (!("null".equals(level_1)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_2)) {
                Double num_1 = Double.parseDouble(split1[0]);
                Double num_3 = Double.parseDouble(split1[2]);
                Double num_4 = Double.parseDouble(split1[3]);
                if ((data_value > num_1 || data_value == num_1) && data_value < num_3) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 1, value.f2, num_1);
                } else if ((data_value > num_3 || data_value == num_3) && data_value < num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                } else if (data_value > num_4 || data_value == num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            } else if (!("null".equals(level_2)) && !("null".equals(level_3)) && !("null".equals(level_4)) && "null".equals(level_1)) {
                Double num_2 = Double.parseDouble(split1[1]);
                Double num_3 = Double.parseDouble(split1[2]);
                Double num_4 = Double.parseDouble(split1[3]);
                if ((data_value > num_2 || data_value == num_2) && data_value < num_3) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 2, value.f2, num_2);
                } else if ((data_value > num_3 || data_value == num_3) && data_value < num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 3, value.f2, num_3);
                } else if (data_value > num_4 || data_value == num_4) {
                    String system_time = String.valueOf(System.currentTimeMillis());
                    result.setFields(value.f0, value.f1, system_time, 4, value.f2, num_4);
                }
            }
        }

        return result;
    }

    public static List<DataStream<AlterStruct>> getOffLineAlarm(DataStream<Tuple2<String, String>> joinResult, BroadcastStream<Map<String, String>> broadcast, TimesConstants build) {
        SingleOutputStreamOperator<AlterStruct> offLine_alert_rule = joinResult.connect(broadcast).process(new getOffLineAlarmProcessFunction());
//        offLine_alert_rule.print("offLine_alert_test:");
        // 收敛规则
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<AlterStruct, ?> alarmGrade =
                Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                        .where(new SimpleCondition<AlterStruct>() {
                            @Override
                            public boolean filter(AlterStruct s) {
                                return s.getLevel().equals("1");
                            }
                        }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("2");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("3");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("4");
                    }
                }).times(build.getOne()).within(Time.seconds(build.getTwo()));
        Pattern<AlterStruct, ?> alarmIncream
                = Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                .where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("1");
                    }
                }).next("middle").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("2");
                    }
                }).next("three").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("3");
                    }
                }).next("finally").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("4");
                    }
                }).times(build.getThree()).within(Time.seconds(build.getFour()));
        PatternStream<AlterStruct> patternStream =
                CEP.pattern(offLine_alert_rule.keyBy(AlterStruct::getGaojing), alarmGrade);
        PatternStream<AlterStruct> alarmIncreamStream =
                CEP.pattern(offLine_alert_rule.keyBy(AlterStruct::getGaojing), alarmIncream);
        DataStream<AlterStruct> alarmStream =
                patternStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(0);
                    }
                });
        DataStream<AlterStruct> alarmStreamIncream =
                alarmIncreamStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(2);
                    }
                });

        List<DataStream<AlterStruct>> list = new ArrayList<>();
        list.add(alarmStream);
        list.add(alarmStreamIncream);
        return list;
    }

    public static AlterStruct getOffLineAlarmResult(Tuple3<String, String, String> value, ReadOnlyBroadcastState<String, String> broadcastState) throws Exception {
        String alarmInfo = broadcastState.get(value.f1).trim();
        // 878.deviceOffline=869|null|117:878:deviceOffline:BM-vds54asdq123:那扫福:null|null|null|0.0:设备已离线
        String[] split = alarmInfo.split(":");
        if (split.length != 7) {
            return null;
        }
        String unique_id = split[0].trim();

        String asset_code = split[3].trim();
        String asset_name = split[4].trim();

        String r_value = split[5].trim();
        if (unique_id.isEmpty() || r_value.isEmpty()) {
            return null;
        }
        String[] split1 = r_value.split("\\|");
        if (split1.length != 4) {
            return null;
        }

        String[] IDS = unique_id.split("\\|");
        String asset_id = IDS[0];
        String indice_id = IDS[1];
        String strategy_id = IDS[2];
        String triger_name = split[6];
        String device_id = split[1];

        String curTime = String.valueOf(System.currentTimeMillis());
        AlterStruct alterStruct = new AlterStruct();

        String level = null;
        for (int i = 0; i < split1.length; i++) {
            if (split1[i] != null) {
                level = String.valueOf(i + 1);
            }
        }

        if (device_id.equals(value.f0)) {
            if (value.f2 == null) {
                String relValue = "0";
                alterStruct = new AlterStruct(device_id, asset_name, triger_name, curTime, relValue, level, unique_id, null, value.f1);
            }
        }
        return alterStruct;
    }
}
