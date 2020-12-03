package com.realleaderAndyanjing.engine;

public class JavaTest {
    public static void main(String[] args) {
        for (int i = 1; i <= 5; i++) {
            for (int j = i + 1; j <= 5; j++) {
                System.out.print(" ");
            }
            for (int z = 1; z <= i; z++) {
                if(i == 4 && z ==2)
                    System.out.print((char)107+" ");
                else if(i == 4 && z == 3)
                    System.out.print((char)98+" ");
                else
                    System.out.print("*" + " ");
            }
            System.out.print("\n");
        }

    }

}
