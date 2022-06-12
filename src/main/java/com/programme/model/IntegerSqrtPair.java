package com.programme.model;

import lombok.AllArgsConstructor;


public class IntegerSqrtPair {
    private Integer value;
    private Double sqrtVal;

    public IntegerSqrtPair(int i){
        value=i;
        sqrtVal = Math.sqrt(i);
    }

}
