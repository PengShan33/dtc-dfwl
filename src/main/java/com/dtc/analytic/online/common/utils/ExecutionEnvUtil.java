package com.dtc.analytic.online.common.utils;

import com.dtc.analytic.online.common.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecutionEnvUtil {
    private static final String EXACTLY_ONCE_MODE = "exactly_once";

    public static ParameterTool createParameterTool(final String[] args) throws IOException {
        return ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromMap(getenv()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Map<String, String> getenv() {
        HashMap<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(),entry.getValue());
        }
        return map;
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String cpMode = parameterTool.get("dtc.checkpointMode", "exactly_once");

        // 设置默认并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM,2));
        // 禁止将作业管理器状态更新打印到系统
        env.getConfig().disableSysoutLogging();
        // 设置重试机制
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000));
        // 设置检查点时间间隔,10s启动一个检查点
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE,true)) {
            env.enableCheckpointing(parameterTool.getInt(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL,10000));
        }
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 设置检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // 设置检查点超时时间,若1分钟内未完成则丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // cancel程序时保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 检查点模式设置
        if (EXACTLY_ONCE_MODE.equals(cpMode)) {
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        }

        return env;
    }
}
