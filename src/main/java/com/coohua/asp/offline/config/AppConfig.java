package com.coohua.asp.offline.config;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by liubin on 2016/4/6.
 */
public class AppConfig implements Serializable {

    public static final AppConfig INSTANCE = new AppConfig();

    String sparkUrl;
    String hdfsPrefix;
    String userAdClickCountPath;
    String accountPath;
    String userValueOutputPath;
    String userPreferenceValueOutputPath;
    String userInviteValueOutputPath;
    String userAllValueResultOutputPath;

    double userValueCpeWeight;
    double userValueCpaWeight;
    double userValueCpmWeight;
    double userValueShareWeight;

    double userPreferenceCpeWeight;
    double userPreferenceCpaWeight;
    double userPreferenceCpmWeight;

    double userInviteValueRatio;

    private AppConfig() {
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void init() throws Exception {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("app.properties");
        Properties properties = new Properties();
        properties.load(inputStream);
        sparkUrl = properties.getProperty("sparkUrl");
        hdfsPrefix = properties.getProperty("hdfsPrefix");
        userAdClickCountPath = properties.getProperty("userAdClickCountPath");
        accountPath = properties.getProperty("accountPath");
        userValueOutputPath = properties.getProperty("userValueOutputPath");
        userPreferenceValueOutputPath = properties.getProperty("userPreferenceOutputPath");
        userInviteValueOutputPath = properties.getProperty("userInviteValueOutputPath");
        userAllValueResultOutputPath = properties.getProperty("userAllValueOutputPath");

        userValueCpeWeight = Double.parseDouble(properties.getProperty("userValueCpeWeight"));
        userValueCpaWeight = Double.parseDouble(properties.getProperty("userValueCpaWeight"));
        userValueCpmWeight = Double.parseDouble(properties.getProperty("userValueCpmWeight"));
        userValueShareWeight = Double.parseDouble(properties.getProperty("userValueShareWeight"));

        userPreferenceCpeWeight = Double.parseDouble(properties.getProperty("userPreferenceCpeWeight"));
        userPreferenceCpaWeight = Double.parseDouble(properties.getProperty("userPreferenceCpaWeight"));
        userPreferenceCpmWeight = Double.parseDouble(properties.getProperty("userPreferenceCpmWeight"));

        userInviteValueRatio = Double.parseDouble(properties.getProperty("userInviteValueRatio"));

        inputStream.close();
    }

    public String getSparkUrl() {
        return sparkUrl;
    }

    public void setSparkUrl(String sparkUrl) {
        this.sparkUrl = sparkUrl;
    }

    public String getHdfsPrefix() {
        return hdfsPrefix;
    }

    public void setHdfsPrefix(String hdfsPrefix) {
        this.hdfsPrefix = hdfsPrefix;
    }

    public String getUserAdClickCountPath() {
        return userAdClickCountPath;
    }

    public void setUserAdClickCountPath(String userAdClickCountPath) {
        this.userAdClickCountPath = userAdClickCountPath;
    }

    public double getUserValueCpeWeight() {
        return userValueCpeWeight;
    }

    public void setUserValueCpeWeight(double userValueCpeWeight) {
        this.userValueCpeWeight = userValueCpeWeight;
    }

    public double getUserValueCpaWeight() {
        return userValueCpaWeight;
    }

    public void setUserValueCpaWeight(double userValueCpaWeight) {
        this.userValueCpaWeight = userValueCpaWeight;
    }

    public double getUserValueCpmWeight() {
        return userValueCpmWeight;
    }

    public void setUserValueCpmWeight(double userValueCpmWeight) {
        this.userValueCpmWeight = userValueCpmWeight;
    }

    public double getUserValueShareWeight() {
        return userValueShareWeight;
    }

    public void setUserValueShareWeight(double userValueShareWeight) {
        this.userValueShareWeight = userValueShareWeight;
    }

    public double getUserPreferenceCpeWeight() {
        return userPreferenceCpeWeight;
    }

    public void setUserPreferenceCpeWeight(double userPreferenceCpeWeight) {
        this.userPreferenceCpeWeight = userPreferenceCpeWeight;
    }

    public double getUserPreferenceCpaWeight() {
        return userPreferenceCpaWeight;
    }

    public void setUserPreferenceCpaWeight(double userPreferenceCpaWeight) {
        this.userPreferenceCpaWeight = userPreferenceCpaWeight;
    }

    public double getUserPreferenceCpmWeight() {
        return userPreferenceCpmWeight;
    }

    public void setUserPreferenceCpmWeight(double userPreferenceCpmWeight) {
        this.userPreferenceCpmWeight = userPreferenceCpmWeight;
    }

    public String getUserValueOutputPath() {
        return userValueOutputPath;
    }

    public void setUserValueOutputPath(String userValueOutputPath) {
        this.userValueOutputPath = userValueOutputPath;
    }

    public String getUserPreferenceValueOutputPath() {
        return userPreferenceValueOutputPath;
    }

    public void setUserPreferenceValueOutputPath(String userPreferenceValueOutputPath) {
        this.userPreferenceValueOutputPath = userPreferenceValueOutputPath;
    }

    public String getUserInviteValueOutputPath() {
        return userInviteValueOutputPath;
    }

    public void setUserInviteValueOutputPath(String userInviteValueOutputPath) {
        this.userInviteValueOutputPath = userInviteValueOutputPath;
    }

    public double getUserInviteValueRatio() {
        return userInviteValueRatio;
    }

    public void setUserInviteValueRatio(double userInviteValueRatio) {
        this.userInviteValueRatio = userInviteValueRatio;
    }

    public String getAccountPath() {
        return accountPath;
    }

    public void setAccountPath(String accountPath) {
        this.accountPath = accountPath;
    }

    public String getUserAllValueResultOutputPath() {
        return userAllValueResultOutputPath;
    }

    public void setUserAllValueResultOutputPath(String userAllValueResultOutputPath) {
        this.userAllValueResultOutputPath = userAllValueResultOutputPath;
    }
}
