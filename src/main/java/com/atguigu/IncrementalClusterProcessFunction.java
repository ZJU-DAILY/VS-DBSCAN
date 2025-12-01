package com.atguigu;

import elki.data.NumberVector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.javatuples.Pair;

import java.util.List;

/**
 * 增量式聚类处理算子：
 * - 输入：当前窗口合并后的聚类结果 Pair<clusters, timestamp>
 * - 在 Flink Keyed State 中保存上一轮的聚类结果
 * - 使用 DBSCANMerger.merge 将“上一轮簇集合”和“当前簇集合”进行合并，实现简单的增量聚类
 * - 输出：合并后的簇集合 Pair<mergedClusters, timestamp>
 *
 * Key 类型可以是任意类型（这里在 MainDemo 中使用常量 key 0L 来保证全局共享一个状态）。
 */
public class IncrementalClusterProcessFunction
        extends KeyedProcessFunction<Long, Pair<List<List<Pair<NumberVector, Integer>>>, Long>,
        Pair<List<List<Pair<NumberVector, Integer>>>, Long>> {

    private final double eps;

    // 保存上一轮聚类结果
    private transient ValueState<List<List<Pair<NumberVector, Integer>>>> prevClustersState;

    public IncrementalClusterProcessFunction(double eps) {
        this.eps = eps;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        ValueStateDescriptor<List<List<Pair<NumberVector, Integer>>>> desc =
                new ValueStateDescriptor<>(
                        "prevClustersState",
                        TypeInformation.of(new TypeHint<List<List<Pair<NumberVector, Integer>>>>() {
                        }));
        prevClustersState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(
            Pair<List<List<Pair<NumberVector, Integer>>>, Long> value,
            KeyedProcessFunction<Long, Pair<List<List<Pair<NumberVector, Integer>>>, Long>,
                    Pair<List<List<Pair<NumberVector, Integer>>>, Long>>.Context ctx,
            Collector<Pair<List<List<Pair<NumberVector, Integer>>>, Long>> out) throws Exception {

        List<List<Pair<NumberVector, Integer>>> currentClusters = value.getValue0();
        Long timestamp = value.getValue1();

        List<List<Pair<NumberVector, Integer>>> prevClusters = prevClustersState.value();

        List<List<Pair<NumberVector, Integer>>> merged;
        if (prevClusters == null) {
            // 首次，没有历史聚类，直接使用当前聚类结果
            merged = currentClusters;
        } else {
            // 将上一轮聚类结果与当前结果合并，作为“增量更新后”的新聚类
            merged = DBSCANMerger.merge(prevClusters, currentClusters, eps);
        }

        // 更新 State 为本次的合并结果
        prevClustersState.update(merged);

        // 向下游输出合并后的聚类结果
        out.collect(new Pair<>(merged, timestamp));
    }
}


