package com.ivy.transportclient.aggregation;

public class BucketModel {
    private String name;
    private Integer height;
    private BucketNestModel nestModel;

    public BucketModel() {
    }

    public BucketModel(String name, Integer height, BucketNestModel nestModel) {
        this.name = name;
        this.height = height;
        this.nestModel = nestModel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public BucketNestModel getNestModel() {
        return nestModel;
    }

    public void setNestModel(BucketNestModel nestModel) {
        this.nestModel = nestModel;
    }
}
