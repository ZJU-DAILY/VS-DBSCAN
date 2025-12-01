package com.atguigu;

import org.apache.flink.api.java.functions.KeySelector;

public class keyByFunc implements KeySelector<data, Integer> {
    private final double[][] zhixin;

    // 构造函数接收质心数组
    public keyByFunc(double[][] zhixin) {
        this.zhixin = zhixin;
    }

    @Override
    public Integer getKey(data value) throws Exception {
        double[] point = value.getAttributes();  // 获取数据点的属性（dim维度的数组）
        int nearestCentroidIndex = findNearestCentroid(point);  // 找到离点最近的质心索引
        return nearestCentroidIndex;  // 返回该质心对应的分区索引
    }

    private int findNearestCentroid(double[] point) {
        int nearestIndex = -1;  // 最近的质心的索引
        double minDistance = Double.MAX_VALUE;  // 用于记录最小的距离

        // 遍历所有质心
        for (int i = 0; i < zhixin.length; i++) {
            double distance = calculateEuclideanDistance(point, zhixin[i]);  // 计算当前点与质心的距离
            if (distance < minDistance) {  // 找到距离更近的质心
                minDistance = distance;
                nearestIndex = i;  // 更新最近的质心索引
            }
        }

        return nearestIndex;  // 返回最近的质心索引
    }

    private double calculateEuclideanDistance(double[] point1, double[] point2) {
        double sum = 0.0;
        for (int i = 0; i < point1.length; i++) {
            sum += Math.pow(point1[i] - point2[i], 2);  // 计算每个维度的平方差并求和
        }
        return Math.sqrt(sum);  // 返回平方和的平方根，即欧几里得距离
    }


}
