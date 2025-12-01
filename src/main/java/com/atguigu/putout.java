package com.atguigu;

import elki.data.NumberVector;
import elki.index.Index;
import org.apache.flink.api.common.functions.MapFunction;
import org.javatuples.Pair;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class putout implements MapFunction<Pair<List<List<Pair<NumberVector,Integer>>>,Long>,String> {

    int[] yTrue;
    int[] yNow;
    Integer dim;
    Integer num;
    Integer minPoints;

    @Override
    public String map(Pair<List<List<Pair<NumberVector,Integer>>>,Long> clusterss) throws Exception {
        String result="";
        System.out.println("\n=== 聚类结果 ===");
        List<Integer> naxie=new LinkedList<>();
        int clusterCounter = 0;
        List<List<Pair<NumberVector, Integer>>> clusters = clusterss.getValue0();
        Long startT = clusterss.getValue1();
        for (List<Pair<NumberVector,Integer>> cluster : clusters) {
            if(cluster.size()>=minPoints){
                System.out.printf("\n 簇 %d (包含 %d 个点)%n",
                        ++clusterCounter, cluster.size());
            }else{
                System.out.printf("\n 噪 声 (包含 %d 个点)%n",
                        cluster.size());
            }

            for(Pair<NumberVector,Integer> p:cluster){
                Integer idx=p.getValue1();
                yNow[idx]=clusterCounter;
                naxie.add(idx);
            }
        }

        int[] yyTrue=new int[naxie.size()];
        int[] yyNow=new int[naxie.size()];
        int xb=0;
        for(Integer xiabiao:naxie){
            yyTrue[xb]=yTrue[xiabiao];
            yyNow[xb]=yNow[xiabiao];
            xb++;
        }
        System.out.printf("\n 总计: %d 个簇%n", clusterCounter-1);

        Long nowT=System.currentTimeMillis();
        System.out.println("用时："+(nowT-startT)+"ms");

//        PrintStream originalOut = System.out;
//        try (PrintStream fileStream = new PrintStream(new FileOutputStream("output.txt", true))) {
//            System.setOut(fileStream);
//            System.out.println(nowT - startT);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            System.setOut(originalOut);
//        }

        double ari = computeARI(yyTrue, yyNow);
        double ami = computeAMI(yyTrue, yyNow);
        System.out.printf("ARI = %.6f%n", ari);
        System.out.printf("AMI = %.6f%n", ami);
//        System.out.println(nowT);
        return result;
    }

    public putout(int[] yTrue, Integer dim, Integer num,Integer minPoints) {
        this.minPoints=minPoints;
        this.dim=dim;
        this.num=num;
        this.yTrue=yTrue;
        yNow=new int[num];
        for (int i = 0; i <num; i++) {
            yNow[i] = -1;
        }
    }

    // ========= ARI 计算 =========
    private static double computeARI(int[] trueL, int[] predL) {
        int n = trueL.length;
        Map<Integer, Integer> trueMap = new HashMap<>(), predMap = new HashMap<>();
        for (int x : trueL)   if (!trueMap.containsKey(x))  trueMap.put(x, trueMap.size());
        for (int x : predL)   if (!predMap.containsKey(x))  predMap.put(x, predMap.size());
        int K = trueMap.size(), M = predMap.size();
        long[][] cont = new long[K][M];
        long[] a = new long[K], b = new long[M];
        for (int i = 0; i < n; i++) {
            int ti = trueMap.get(trueL[i]), pj = predMap.get(predL[i]);
            cont[ti][pj]++; a[ti]++; b[pj]++;
        }
        double sumComb = 0, sumAi = 0, sumBj = 0, totalComb = comb2(n);
        for (int i = 0; i < K; i++) sumAi += comb2(a[i]);
        for (int j = 0; j < M; j++) sumBj += comb2(b[j]);
        for (int i = 0; i < K; i++)
            for (int j = 0; j < M; j++)
                sumComb += comb2(cont[i][j]);
        double expected = sumAi * sumBj / totalComb;
        double maxIndex = 0.5 * (sumAi + sumBj);
        return (sumComb - expected) / (maxIndex - expected);
    }

    private static double comb2(double x) {
        return x * (x - 1) / 2.0;
    }

    // ========= AMI 计算 =========
    private static double computeAMI(int[] trueL, int[] predL) {
        int n = trueL.length;
        Map<Integer, Integer> trueMap = new HashMap<>(), predMap = new HashMap<>();
        for (int x : trueL)   if (!trueMap.containsKey(x))  trueMap.put(x, trueMap.size());
        for (int x : predL)   if (!predMap.containsKey(x))  predMap.put(x, predMap.size());
        int K = trueMap.size(), M = predMap.size();
        int[][] cont = new int[K][M];
        int[] a = new int[K], b = new int[M];
        for (int i = 0; i < n; i++) {
            int ti = trueMap.get(trueL[i]), pj = predMap.get(predL[i]);
            cont[ti][pj]++; a[ti]++; b[pj]++;
        }
        // MI
        double mi = 0;
        for (int i = 0; i < K; i++)
            for (int j = 0; j < M; j++)
                if (cont[i][j] > 0)
                    mi += (cont[i][j]/(double)n) *
                            Math.log((cont[i][j]*(double)n)/(a[i]*(double)b[j]));

        // H(U), H(V)
        double hTrue = 0, hPred = 0;
        for (int v : a) if (v>0) { double p=v/(double)n; hTrue -= p*Math.log(p); }
        for (int v : b) if (v>0) { double p=v/(double)n; hPred -= p*Math.log(p); }

        // EMI
        double[] logFact = precomputeLogFactorial(n);
        double emi = 0;
        for (int i = 0; i < K; i++) {
            for (int j = 0; j < M; j++) {
                int ai = a[i], bj = b[j];
                int lo = Math.max(1, ai + bj - n), hi = Math.min(ai, bj);
                for (int nij = lo; nij <= hi; nij++) {
                    double logP =
                            logFact[ai] - logFact[nij] - logFact[ai-nij]
                                    + logFact[n - ai]
                                    - logFact[bj-nij] - logFact[n - ai - (bj-nij)]
                                    - (logFact[n] - logFact[bj] - logFact[n-bj]);
                    double p = Math.exp(logP);
                    emi += p * (nij/(double)n) *
                            Math.log((nij*(double)n)/(ai*(double)bj));
                }
            }
        }

        double denom = ((hTrue + hPred)/2.0) - emi;
        return (mi - emi) / (denom == 0 ? 1 : denom);
    }

    private static double[] precomputeLogFactorial(int n) {
        double[] lf = new double[n+1];
        lf[0] = 0;
        for (int i = 1; i <= n; i++) lf[i] = lf[i-1] + Math.log(i);
        return lf;
    }
}
