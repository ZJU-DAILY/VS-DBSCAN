package com.atguigu;

import elki.data.NumberVector;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.javatuples.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.atguigu.HaltonSequence.generate;

public class MainDemo {

    private static Integer zhixinnum;
    private static Integer dim;
    private static double zhixin[][];
    private static double eps;
    private static int minPoints;
    private static int N;

    public static void main(String[] args) throws Exception {

        zhixinnum=10;
        eps=0.31;
        minPoints=21;
        dim=20;
        Long nowT=System.currentTimeMillis();
        System.out.println(nowT);
        String filePath = "D:\\idea\\program\\FlinkTutorial-1.17\\src\\main\\java\\com\\atguigu\\datas\\mnist_newone.txt";//每行一个数据，格式为：“标签,属性1,属性2,...,属性n,编号”，编号为该数据在数据集中是第几个（1-n）
        List<String[]> rawLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                rawLines.add(line.trim().split(","));
            }
        }
        N = rawLines.size();
        int[] yTrue = new int[N];
        double[][] rawFeat = new double[N][dim];
        for (int i = 0; i < N; i++) {
            String[] tok = rawLines.get(i);
            yTrue[i] = Integer.parseInt(tok[0].trim());
        }

        zhixin=new double[zhixinnum][dim];
        for (int i = 0; i < zhixinnum; i++) {
            zhixin[i] = generate(i+1, dim);
            for(int j=0;j<zhixin[i].length;j++){
                zhixin[i][j]=zhixin[i][j]*2-1;
            }
        }

        //flink
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(10);
        DataStreamSource<String> source1 = env.readTextFile(filePath);
        SingleOutputStreamOperator<String> throttledSource = source1.map(new RichMapFunction<String, String>() {
            private transient long recordCounter = 0L; // 计数器

            @Override
            public String map(String value) throws Exception {
                recordCounter++; // 处理记录数+1

                if(recordCounter%2==0)
                    Thread.sleep(1,0);

                return value;
            }
        });

        SingleOutputStreamOperator<data> source2 = throttledSource.map(new InputFunc());

        WatermarkStrategy<data> dataWatermarkStrategy = WatermarkStrategy
                .<data>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<data>() {
                    @Override
                    public long extractTimestamp(data element, long recordTimestamp) {
                        return element.timestamp;
                    }
                });

        SingleOutputStreamOperator<data> source = source2.assignTimestampsAndWatermarks(dataWatermarkStrategy);

        // 使用基于 LSH 的分区器进行 keyBy，将高维向量映射为一个整数分区 ID
        int numHashBits = 10;   // 10 位签名 => 1024 个 bucket，足以覆盖当前并行度
        long lshSeed = 42L;     // 固定随机种子，保证各 Task 上 LSH 一致
        KeyedStream<data, Integer> keyedStream = source.keyBy(new LSHKeySelector(numHashBits, dim, lshSeed));
        SingleOutputStreamOperator<myCluster> process = keyedStream
                .window(SlidingProcessingTimeWindows.of(Time.seconds(3),Time.seconds(2)))
                .process(
                        new ProcessWindowFunction<data, myCluster, Integer, TimeWindow>() {

                            public void process(
                                    Integer integer, ProcessWindowFunction<data, myCluster, Integer, TimeWindow>.Context context, Iterable<data> elements, Collector<myCluster> out) throws Exception {
                                DBSCANFunc myDBSCANFunc = new DBSCANFunc(eps, minPoints, elements);
                                myCluster nowCluster = myDBSCANFunc.dbscanCluster();
                                nowCluster.timestamp = context.window().getEnd();
                                out.collect(nowCluster);
                            }
                        });


        KeyedStream<Pair<List<List<Pair<NumberVector, Integer>>>, Long>, Long> TimeKeyedStream = process.map(new MapFunction<myCluster, Pair<List<List<Pair<NumberVector, Integer>>>, Long>>() {
            @Override
            public Pair<List<List<Pair<NumberVector, Integer>>>, Long> map(myCluster value) throws Exception {
                Pair<List<List<Pair<NumberVector, Integer>>>, Long> now = new Pair<>(value.clusters, value.timestamp);
                return now;
            }
        }).keyBy(new MergerKeyby());

        SingleOutputStreamOperator<Pair<List<List<Pair<NumberVector, Integer>>>, Long>> result = TimeKeyedStream.reduce(new ReduceFunction<Pair<List<List<Pair<NumberVector, Integer>>>, Long>>() {
            @Override
            public Pair<List<List<Pair<NumberVector, Integer>>>, Long> reduce(Pair<List<List<Pair<NumberVector, Integer>>>, Long> value1, Pair<List<List<Pair<NumberVector, Integer>>>, Long> value2) throws Exception {
                Pair<List<List<Pair<NumberVector, Integer>>>, Long> lists = new Pair<>(DBSCANMerger.merge(value2.getValue0(), value1.getValue0(), eps), value1.getValue1());
                return lists;
            }
        });

        // 基于 Flink State 的增量式聚类：
        // 将所有全局聚类结果再 keyBy 到同一个 key (0L)，在 IncrementalClusterProcessFunction 中
        // 使用 ValueState 保存上一轮簇集合，并与当前窗口结果进行合并，实现简单的增量聚类。
        SingleOutputStreamOperator<Pair<List<List<Pair<NumberVector, Integer>>>, Long>> incrementalResult =
                result
                        .keyBy(value -> 0L)
                        .process(new IncrementalClusterProcessFunction(eps));

        incrementalResult.map(new putout(yTrue,dim,N,minPoints)).print();

        env.execute();
    }
}