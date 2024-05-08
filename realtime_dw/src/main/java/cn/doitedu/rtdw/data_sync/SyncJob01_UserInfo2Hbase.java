package cn.doitedu.rtdw.data_sync;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SyncJob01_UserInfo2Hbase {
    public static void main(String[] args) {
        // flink程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/e:/test_doitedu");

        // 构造table编程环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        
    }
}
