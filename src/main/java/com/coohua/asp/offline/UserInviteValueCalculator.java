package com.coohua.asp.offline;

import com.coohua.asp.offline.bean.AdCountItem;
import com.coohua.asp.offline.config.AppConfig;
import com.coohua.asp.offline.utils.LogParserUtils;
import org.apache.commons.lang3.StringUtils;
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
public class UserInviteValueCalculator {

    public static JavaRDD<Tuple2<String, Double>> calcUserInviteValue(JavaPairRDD<String, Tuple2<AdCountItem, String>> userInviteRdd) {
        final AppConfig appConfig = AppConfig.INSTANCE;
        JavaPairRDD<String, Double> userInviteValueRdd = userInviteRdd.mapToPair(new PairFunction<Tuple2<String, Tuple2<AdCountItem, String>>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<AdCountItem, String>> v1) throws Exception {
                String inviteCoohuaId = v1._2()._2();
                AdCountItem adCountItem = v1._2()._1();
                double sum = adCountItem.getCpaLeft() * appConfig.getUserValueCpaWeight() +
                        adCountItem.getCpeLeft() * appConfig.getUserValueCpeWeight() +
                        adCountItem.getCpmLeft() * appConfig.getUserValueCpeWeight() +
                        adCountItem.getShareLeft() * appConfig.getUserValueShareWeight();
                return new Tuple2<String, Double>(inviteCoohuaId, sum);
            }
        });
        JavaRDD<Tuple2<String, Double>> result = userInviteValueRdd.groupByKey().map(new Function<Tuple2<String, Iterable<Double>>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> v1) throws Exception {
                Double sum = 0.0;
                for (double value : v1._2()) {
                    sum += value;
                }
                return new Tuple2<String, Double>(v1._1(), sum);
            }
        });
        return result;
    }

    public static void processAndSave(JavaPairRDD<String, AdCountItem> adCountItemJavaRDD, JavaPairRDD<String, String> inviteRdd, String day) {
        final AppConfig appConfig = AppConfig.INSTANCE;
        JavaPairRDD<String, Tuple2<AdCountItem, String>> userInviteRdd = adCountItemJavaRDD.join(inviteRdd, 10);
        JavaRDD<Tuple2<String, Double>> javaRDD = calcUserInviteValue(userInviteRdd);
        JavaRDD<String> saveRdd = javaRDD.map(new Function<Tuple2<String, Double>, String>() {
            @Override
            public String call(Tuple2<String, Double> v1) throws Exception {
                return v1._1() + "\t" + v1._2();
            }
        });
        saveRdd.saveAsTextFile(appConfig.getUserInviteValueOutputPath() + "/" + day);
    }

    public static JavaPairRDD<String, Double> processAndGetPairValue(JavaPairRDD<String, AdCountItem> adCountItemJavaPairRDD, JavaPairRDD<String, String> inviteRdd, String day) {
        JavaPairRDD<String, Tuple2<AdCountItem, String>> userInviteRdd = adCountItemJavaPairRDD.join(inviteRdd, 10);
        JavaRDD<Tuple2<String, Double>> javaRDD = calcUserInviteValue(userInviteRdd);
        JavaPairRDD<String, Double> resultRdd = javaRDD.mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return stringDoubleTuple2;
            }
        });
        return resultRdd;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp xx.jar com.coohua.asp.offline.UserInviteValueCalculator [day]");
            System.exit(-1);
        }

        String day = args[0];
        AppConfig appConfig = AppConfig.INSTANCE;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("userInviteValueCalculator");
        sparkConf.setMaster(appConfig.getSparkUrl());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = sparkContext.textFile(appConfig.getUserAdClickCountPath() + "/" + day, 5);
        JavaPairRDD<String, AdCountItem> adCountItemJavaRDD = file.mapToPair(new PairFunction<String, String, AdCountItem>() {
            @Override
            public Tuple2<String, AdCountItem> call(String s) throws Exception {
                AdCountItem adCountItem = LogParserUtils.parseAdCountItem(s);
                return new Tuple2<String, AdCountItem>(adCountItem.getCoohuaId(), adCountItem);
            }
        });
        JavaRDD<String> accountFile = sparkContext.textFile(appConfig.getAccountPath(), 10);
        JavaPairRDD<String, String> inviteRdd = accountFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tokens = StringUtils.split(s, ",");
                return new Tuple2<String, String>(tokens[0], tokens[1]);
            }
        });
        UserInviteValueCalculator.processAndSave(adCountItemJavaRDD, inviteRdd, day);
        sparkContext.stop();
    }
}
