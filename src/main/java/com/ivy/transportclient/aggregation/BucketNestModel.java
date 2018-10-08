package com.ivy.transportclient.aggregation;

public class BucketNestModel {
    private Integer nestHeight;

    public BucketNestModel() {
    }

    public BucketNestModel(Integer nestHeight) {
        this.nestHeight = nestHeight;
    }

    public Integer getNestHeight() {
        return nestHeight;
    }

    public void setNestHeight(Integer nestHeight) {
        this.nestHeight = nestHeight;
    }
}
