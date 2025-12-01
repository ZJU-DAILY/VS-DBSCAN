package com.atguigu;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.Random;

/**
 * 基于随机超平面（Random Hyperplane LSH）的 KeySelector，用于将高维向量映射到离散分区 ID。
 *
 * 将 dim 维向量投影到若干随机超平面上，符号模式组成一个 bit-signature，最后转换为整数作为 key。
 */
public class LSHKeySelector implements KeySelector<data, Integer> {

    /** 随机超平面个数（也就是 hash 位数），决定 bucket 数量为 2^numHashBits */
    private final int numHashBits;

    /** 向量维度 */
    private final int dim;

    /** 随机超平面向量，shape: [numHashBits][dim] */
    private final double[][] randomHyperplanes;

    /**
     * @param numHashBits 使用多少个随机超平面（即多少位的 LSH 签名）
     * @param dim         向量维度
     * @param seed        随机种子，保证在所有 Task 上 hash 一致可复现
     */
    public LSHKeySelector(int numHashBits, int dim, long seed) {
        if (numHashBits <= 0) {
            throw new IllegalArgumentException("numHashBits 必须为正数");
        }
        if (dim <= 0) {
            throw new IllegalArgumentException("dim 必须为正数");
        }
        this.numHashBits = numHashBits;
        this.dim = dim;
        this.randomHyperplanes = new double[numHashBits][dim];
        initRandomHyperplanes(seed);
    }

    private void initRandomHyperplanes(long seed) {
        Random random = new Random(seed);
        for (int i = 0; i < numHashBits; i++) {
            double normSq = 0.0;
            for (int j = 0; j < dim; j++) {
                // 生成服从 N(0,1) 的随机向量近似：Box-Muller 或直接使用 nextGaussian
                double v = random.nextGaussian();
                randomHyperplanes[i][j] = v;
                normSq += v * v;
            }
            // 归一化向量，避免数值过大
            double norm = Math.sqrt(normSq);
            if (norm > 0) {
                for (int j = 0; j < dim; j++) {
                    randomHyperplanes[i][j] /= norm;
                }
            }
        }
    }

    @Override
    public Integer getKey(data value) {
        double[] point = value.getAttributes();
        if (point == null || point.length != dim) {
            throw new IllegalArgumentException("输入数据维度与 LSH 配置不一致，期望 dim=" + dim);
        }
        int signature = 0;
        // 逐个超平面计算点到该超平面的侧别，>0 记为 1，否则 0
        for (int i = 0; i < numHashBits; i++) {
            double dot = 0.0;
            double[] plane = randomHyperplanes[i];
            for (int d = 0; d < dim; d++) {
                dot += point[d] * plane[d];
            }
            // 将 bit 拼成一个整数（注意 Java int 至多 32 位）
            if (dot >= 0) {
                signature |= (1 << i);
            }
        }
        // Flink 要求 key 为非 null；这里直接使用 signature 作为分区 key
        return signature;
    }
}


