package com.google.cloud.solutions.realtimedash.pipeline;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BranchCompanySkuValue {

    public BranchCompanySkuValue(String branch, String company, String sku, String value) {
        this.branch = branch;
        this.company = company;
        this.sku = sku;
        this.value = value;
    }

    private String branch;
    private String company;
    private String sku;
    private String value;

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}