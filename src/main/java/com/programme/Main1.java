package com.programme;

public class Main1 {
    public static void main(String[] args) {
        Main1 main = new Main1();
        int a= 5;
        main.abc(a);
        System.out.println("after main call :"+a);
    }

    public void abc(int a){
        System.out.println("before set "+a);
        a= 10;
        System.out.println("after set "+a);
    }
}
