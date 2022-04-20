package com.lp.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SourceApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        addMysqlSource(env);
        env.execute();
    }

    public static void addMysqlSource(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSource());
        source.print();
    }
}
