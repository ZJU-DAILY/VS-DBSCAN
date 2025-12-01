package com.atguigu;

import org.apache.flink.api.common.functions.MapFunction;

public class InputFunc implements MapFunction<String, data> {

    public data map(String value) throws Exception {
        int pos;
        String[] split = value.split(",");
        double[] mydata = new double[split.length-2];
        for (int i = 1; i < split.length-1; i++) {
            mydata[i-1] = Double.parseDouble(split[i]);
        }
        pos=Integer.parseInt(split[split.length-1])-1;
        data newData=new data(mydata,pos,split.length-2);
        return newData;
    }

}