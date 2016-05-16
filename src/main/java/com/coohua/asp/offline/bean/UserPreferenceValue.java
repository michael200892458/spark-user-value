package com.coohua.asp.offline.bean;

import java.io.Serializable;

/**
 * Created by liubin on 2016/4/7.
 */
public class UserPreferenceValue implements Serializable {
    private double cpeValue;
    private double cpmValue;
    private double cpaValue;

    public double getCpeValue() {
        return cpeValue;
    }

    public void setCpeValue(double cpeValue) {
        this.cpeValue = cpeValue;
    }

    public double getCpmValue() {
        return cpmValue;
    }

    public void setCpmValue(double cpmValue) {
        this.cpmValue = cpmValue;
    }

    public double getCpaValue() {
        return cpaValue;
    }

    public void setCpaValue(double cpaValue) {
        this.cpaValue = cpaValue;
    }
}
