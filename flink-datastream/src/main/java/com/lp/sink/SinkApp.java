package com.lp.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        toMySql(env);
        env.execute("SinkApp");
    }

    private static void toMySql(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.valueOf(splits[0].trim());
                String domian = splits[1].trim();
                Double traffic = Double.valueOf(splits[2].trim());
                return new Access(time, domian, traffic);
            }
        });
        // 1.根据domian keyby
        // 2.保存domian, traffic
        SingleOutputStreamOperator<Access> result = mapStream.keyBy(new KeySelector<Access, String>() {
            @Override
            public String getKey(Access access) throws Exception {
                return access.getDomain();
            }
        }).sum("traffic");

//        result.print();
        result.map(new MapFunction<Access, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Access access) throws Exception {
                String domian = access.getDomain();
                Double traffic = access.getTraffic();
                return new Tuple2<>(domian, traffic);
            }
        }).addSink(new PKMySqlSink());
    }
}
