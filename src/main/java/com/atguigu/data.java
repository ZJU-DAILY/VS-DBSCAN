package com.atguigu;

import java.util.Arrays;

public class data {
    public double[] attributes;
    public long timestamp;  // 添加时间戳属性
    public Integer dim;
    public Integer pos;

    public data(double[] inputArray,Integer pos,Integer dim) {
        this.dim=dim;
        this.pos=pos;
        if (inputArray == null) {
            throw new IllegalArgumentException("输入数组不能为null");
        }

        if (inputArray.length != dim) {
            throw new IllegalArgumentException("输入数组长度必须为?");
        }
        this.attributes = new double[dim];
        // 深拷贝传入数组，防止外部修改影响内部数据[10,11](@ref)
        System.arraycopy(inputArray, 0, this.attributes, 0, dim);

        // 设置时间戳为当前系统时间
        this.timestamp = System.currentTimeMillis();
    }

    public double[] getAttributes() {
        return attributes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {

        return "data{" +
                "attributes=" + Arrays.toString(attributes) +
                ", pos=" + pos +
                '}';
    }

    public void setAttributes(double[] attributes) {
        this.attributes = attributes;
    }
}
