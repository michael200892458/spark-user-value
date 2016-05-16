package com.coohua.asp.offline.utils;

import com.coohua.asp.offline.bean.AdCountItem;
import com.coohua.asp.offline.bean.UserPreferenceValue;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by liubin on 2016/4/6.
 */
public class LogParserUtils {
    public static AdCountItem parseAdCountItem(String line) {
        if (StringUtils.isBlank(line)) {
            return null;
        }
        try {
            String[] tokens = StringUtils.split(StringUtils.trim(line), "\t");
            AdCountItem adCountItem = new AdCountItem();
            if (tokens.length < 11) {
                return null;
            }
            adCountItem.setCoohuaId(tokens[0]);
            adCountItem.setTotalLeft(Long.parseLong(tokens[1]));
            adCountItem.setTotalRight(Long.parseLong(tokens[2]));
            adCountItem.setCpaLeft(Long.parseLong(tokens[3]));
            adCountItem.setCpaRight(Long.parseLong(tokens[4]));
            adCountItem.setCpeLeft(Long.parseLong(tokens[5]));
            adCountItem.setCpeRight(Long.parseLong(tokens[6]));
            adCountItem.setCpmLeft(Long.parseLong(tokens[7]));
            adCountItem.setCpmRight(Long.parseLong(tokens[8]));
            adCountItem.setShareLeft(Long.parseLong(tokens[9]));
            adCountItem.setShareRight(Long.parseLong(tokens[10]));
            return adCountItem;
        } catch (Exception e) {
            return null;
        }
    }

    public static String toString(AdCountItem adCountItem) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(adCountItem.getCoohuaId()).append("\t")
                .append(adCountItem.getTotalLeft()).append("\t")
                .append(adCountItem.getTotalRight()).append("\t")
                .append(adCountItem.getCpaLeft()).append("\t")
                .append(adCountItem.getCpaRight()).append("\t")
                .append(adCountItem.getCpeLeft()).append("\t")
                .append(adCountItem.getCpeRight()).append("\t")
                .append(adCountItem.getCpmLeft()).append("\t")
                .append(adCountItem.getCpmRight()).append("\t")
                .append(adCountItem.getShareLeft()).append("\t")
                .append(adCountItem.getShareRight());
        return stringBuilder.toString();
    }

    public static String toString(AdCountItem adCountItem, double userValue) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(toString(adCountItem)).append("\t").append(userValue);
        return stringBuilder.toString();
    }

    public static String toString(AdCountItem adCountItem, UserPreferenceValue userPreferenceValue) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(toString(adCountItem)).append("\t")
                .append(userPreferenceValue.getCpaValue()).append("\t")
                .append(userPreferenceValue.getCpeValue()).append("\t")
                .append(userPreferenceValue.getCpmValue());
        return stringBuilder.toString();
    }
}
