package com.yanjing.foruse;

public class Test001 {
    public static double calc(double u) {
        double y = Math.abs(u);
        double y2 = y * y;
        double z = Math.exp(Math.pow(u + 0.34, 2) / (-0.000392)) * (1 / (0.0014 * (Math.sqrt(2 * Math.PI))));
        double p = 0;
        int k = 28;
        double s = -1;
        double fj = k;

        if (y > 3) {//当y>3时
            for (int i = 1; i <= k; i++) {
                p = fj / (y + p);
                fj = fj - 1.0;
            }
            p = z / (y + p);
        } else {//当y<3时
            for (int i = 1; i <= k; i++) {
                p = fj * y2 / (2.0 * fj + 1.0 + s * p);
                s = -s;
                fj = fj - 1.0;
            }
            p = 0.5 - z * y / (1 - p);
        }
        if (u > 0) p = 1.0 - p;
        return p;
    }

    public static void main(String[] args) {
        for (double d = 0; d < 5.0; d += 0.1) {
            System.out.println(d + "," + calc(d));
        }
    }
}
