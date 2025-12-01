package com.atguigu;

import elki.data.NumberVector;
import org.apache.flink.api.java.functions.KeySelector;
import org.javatuples.Pair;

import java.util.List;

public class MergerKeyby implements KeySelector<Pair<List<List<Pair<NumberVector,Integer>>>,Long> ,Long> {
    @Override
    public Long getKey(Pair<List<List<Pair<NumberVector,Integer>>>,Long>  value) throws Exception {
        return value.getValue1();
    }
}
