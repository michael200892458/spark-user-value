package com.coohua.asp.offline.bean;

import java.io.Serializable;

/**
 * Created by liubin on 2016/4/6.
 */
public class AdCountItem implements Serializable {
    private String coohuaId;
    private long totalLeft;
    private long totalRight;
    private long cpaLeft;
    private long cpaRight;
    private long cpeLeft;
    private long cpeRight;
    private long cpmLeft;
    private long cpmRight;
    private long shareLeft;
    private long shareRight;

    public String getCoohuaId() {
        return coohuaId;
    }

    public void setCoohuaId(String coohuaId) {
        this.coohuaId = coohuaId;
    }

    public long getTotalLeft() {
        return totalLeft;
    }

    public void setTotalLeft(long totalLeft) {
        this.totalLeft = totalLeft;
    }

    public long getTotalRight() {
        return totalRight;
    }

    public void setTotalRight(long totalRight) {
        this.totalRight = totalRight;
    }

    public long getCpaLeft() {
        return cpaLeft;
    }

    public void setCpaLeft(long cpaLeft) {
        this.cpaLeft = cpaLeft;
    }

    public long getCpaRight() {
        return cpaRight;
    }

    public void setCpaRight(long cpaRight) {
        this.cpaRight = cpaRight;
    }

    public long getCpeLeft() {
        return cpeLeft;
    }

    public void setCpeLeft(long cpeLeft) {
        this.cpeLeft = cpeLeft;
    }

    public long getCpeRight() {
        return cpeRight;
    }

    public void setCpeRight(long cpeRight) {
        this.cpeRight = cpeRight;
    }

    public long getCpmLeft() {
        return cpmLeft;
    }

    public void setCpmLeft(long cpmLeft) {
        this.cpmLeft = cpmLeft;
    }

    public long getCpmRight() {
        return cpmRight;
    }

    public void setCpmRight(long cpmRight) {
        this.cpmRight = cpmRight;
    }

    public long getShareLeft() {
        return shareLeft;
    }

    public void setShareLeft(long shareLeft) {
        this.shareLeft = shareLeft;
    }

    public long getShareRight() {
        return shareRight;
    }

    public void setShareRight(long shareRight) {
        this.shareRight = shareRight;
    }
}
