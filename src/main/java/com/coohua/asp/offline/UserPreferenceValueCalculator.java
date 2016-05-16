package com.coohua.asp.offline;

import com.coohua.asp.offline.bean.AdCountItem;
import com.coohua.asp.offline.bean.UserPreferenceValue;
import com.coohua.asp.offline.config.AppConfig;
import com.coohua.asp.offline.utils.LogParserUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by liubin on 2016/4/7.
 */
public class UserPreferenceValueCalculator {
    public static JavaRDD<Tuple2<String, UserPreferenceValue>> calcUserPreferenceValue(JavaRDD<AdCountItem> adCountItemJavaRDD) {
        final AppConfig appConfig = AppConfig.INSTANCE;
        return adCountItemJavaRDD.map(new Function<AdCountItem, Tuple2<String, UserPreferenceValue>>() {
             @Override
             public Tuple2<String, UserPreferenceValue> call(AdCountItem v1) throws Exception {
                 UserPreferenceValue userPreferenceValue = new UserPreferenceValue();
                 double cpaSum = appConfig.getUserPreferenceCpaWeight() * v1.getCpaLeft();
                 double cpeSum = appConfig.getUserPreferenceCpeWeight() * v1.getCpeLeft();
                 double cpmSum = appConfig.getUserPreferenceCpmWeight() * v1.getCpmLeft();
                 double sum = cpaSum + cpeSum + cpmSum;
                 if (sum > 1e-6) {
                     userPreferenceValue.setCpaValue(cpaSum / sum);
                     userPreferenceValue.setCpeValue(cpeSum / sum);
                     userPreferenceValue.setCpmValue(cpmSum / sum);
                 }
                 return new Tuple2<String, UserPreferenceValue>(v1.getCoohuaId(), userPreferenceValue);
             }
         });
    }

    public static void processAndSave(JavaRDD<AdCountItem> adCountItemJavaRDD, String day) {
        AppConfig appConfig = AppConfig.INSTANCE;
        JavaRDD<Tuple2<String, UserPreferenceValue>> userPreferenceRDD = calcUserPreferenceValue(adCountItemJavaRDD);
        JavaRDD<String> resultRdd = userPreferenceRDD.map(new Function<Tuple2<String, UserPreferenceValue>, String>() {
            @Override
            public String call(Tuple2<String, UserPreferenceValue> v1) throws Exception {
                return v1._1() + "\t" + v1._2().getCpaValue() + "\t" + v1._2().getCpeValue() + "\t" + v1._2().getCpmValue();
            }
        });
        resultRdd.saveAsTextFile(appConfig.getUserPreferenceValueOutputPath() + "/" + day);
    }

    public static JavaPairRDD<String, String> processAndGetPairValue(JavaRDD<AdCountItem> adCountItemJavaRDD, String day) {
        final JavaRDD<Tuple2<String, UserPreferenceValue>> userPreferenceRDD = calcUserPreferenceValue(adCountItemJavaRDD);
        JavaPairRDD<String, String> resultRdd = userPreferenceRDD.mapToPair(new PairFunction<Tuple2<String, UserPreferenceValue>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, UserPreferenceValue> stringUserPreferenceValueTuple2) throws Exception {
                UserPreferenceValue userPreferenceValue = stringUserPreferenceValueTuple2._2();
                String value = userPreferenceValue.getCpaValue() + "\t" + userPreferenceValue.getCpeValue() + "\t" + userPreferenceValue.getCpmValue();
                return new Tuple2<String, String>(stringUserPreferenceValueTuple2._1(), value);
            }
        });
        return resultRdd;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp xx.jar com.coohua.asp.offline.UserPreferenceValueCalculator [day]");
            System.exit(-1);
        }
        String day = args[0];
        AppConfig appConfig = AppConfig.INSTANCE;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("UserPreferenceValueCalculator");
        sparkConf.setMaster(appConfig.getSparkUrl());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = sparkContext.textFile(appConfig.getUserAdClickCountPath() + "/" + day, 5);
        JavaRDD<AdCountItem> adCountItemJavaRDD = file.map(new Function<String, AdCountItem>() {
            @Override
            public AdCountItem call(String v1) throws Exception {
                return LogParserUtils.parseAdCountItem(v1);
            }
        });
        UserPreferenceValueCalculator userPreferenceValueCalculator = new UserPreferenceValueCalculator();
        userPreferenceValueCalculator.processAndSave(adCountItemJavaRDD, day);
        sparkContext.stop();
    }
}
