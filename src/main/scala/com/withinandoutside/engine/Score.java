package com.withinandoutside.engine;

/**
 * WanWan MCTSTree数据结构中属性Score:
 * 包含A、B区间,所有皆为[],左闭右闭区间
 */
public class Score {
    private double[] a;
    private double[] b;

    public Score(double[] a, double[] b) {
        this.a = a;
        this.b = b;
    }

    public double[] getA() {
        return a;
    }

    public void setA(double[] a) {
        this.a = a;
    }

    public double[] getB() {
        return b;
    }

    public void setB(double[] b) {
        this.b = b;
    }
}
