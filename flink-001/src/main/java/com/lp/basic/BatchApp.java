package com.lp.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.concurrent.ExecutionException;

public class BatchApp {
    public static void main(String[] args) throws Exception {
        // 上下文
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // source
        DataSource<String> source = env.readTextFile("data/wc.data");
        // transformation
        source.flatMap(new PKFlatMapFunction())
                .map(new PKMapFunction())
                .groupBy(0)
                .sum(1)
                .print();
    }
}

class PKFlatMapFunction implements FlatMapFunction<String, String>{

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for (String word: words){
            out.collect(word.toLowerCase().trim());
        }
    }
}

class PKMapFunction implements MapFunction<String, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value, 1);
    }
}
