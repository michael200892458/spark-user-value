package com.coohua.asp.offline;

import com.coohua.asp.offline.bean.AdCountItem;
import com.coohua.asp.offline.config.AppConfig;
import com.coohua.asp.offline.utils.LogParserUtils;
import com.google.common.base.Optional;
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
public class JobControl {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp xx.jar com.coohua.asp.offline.JobControl [day]");
            System.exit(-1);
        }

        String day = args[0];
        AppConfig appConfig = AppConfig.INSTANCE;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("userValueJobControl");
        sparkConf.setMaster(appConfig.getSparkUrl());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = sparkContext.textFile(appConfig.getUserAdClickCountPath() + "/" + day, 5);
        JavaRDD<String> accountFile = sparkContext.textFile(appConfig.getAccountPath(), 10);
        JavaRDD<AdCountItem> adCountItemJavaRDD = file.map(new Function<String, AdCountItem>() {
            @Override
            public AdCountItem call(String v1) throws Exception {
                return LogParserUtils.parseAdCountItem(v1);
            }
        });
        adCountItemJavaRDD.cache();
        JavaPairRDD<String, AdCountItem> adCountItemJavaPairRDD = adCountItemJavaRDD.mapToPair(new PairFunction<AdCountItem, String, AdCountItem>() {
            @Override
            public Tuple2<String, AdCountItem> call(AdCountItem adCountItem) throws Exception {
                return new Tuple2<String, AdCountItem>(adCountItem.getCoohuaId(), adCountItem);
            }
        });
        JavaPairRDD<String, String> inviteMapRdd = accountFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tokens = StringUtils.split(s, ",");
                return new Tuple2<String, String>(tokens[0], tokens[1]);
            }
        });
        JavaPairRDD<String, Double> userValueRdd = UserValueCalculator.processAndGetPairValue(adCountItemJavaRDD, day);
        JavaPairRDD<String, String> userPreferenceRdd = UserPreferenceValueCalculator.processAndGetPairValue(adCountItemJavaRDD, day);
        JavaPairRDD<String, Double> userInviteValueRdd = UserInviteValueCalculator.processAndGetPairValue(adCountItemJavaPairRDD, inviteMapRdd, day);
        JavaPairRDD<String, Tuple2<Double, String>> userValueAndPreferenceValue = userValueRdd.join(userPreferenceRdd);
        JavaPairRDD<String, Tuple2<Tuple2<Double, String>, Optional<Double>>> allValueRDD = userValueAndPreferenceValue.leftOuterJoin(userInviteValueRdd);
        JavaRDD<String> resultRdd = allValueRDD.map(new Function<Tuple2<String, Tuple2<Tuple2<Double, String>, Optional<Double>>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<Tuple2<Double, String>, Optional<Double>>> v1) throws Exception {
                String key = v1._1();
                Double userValue = v1._2()._1()._1();
                String userPreferenceValue = v1._2()._1()._2();
                Double userInviteValue = new Double(0);
                if (v1._2()._2().isPresent()) {
                    userInviteValue = v1._2()._2().get();
                }
                return key + "\t" + userValue + "\t" + userPreferenceValue + "\t" + userInviteValue;
            }
        });
        resultRdd.saveAsTextFile(appConfig.getUserAllValueResultOutputPath() + "/" + day);
        sparkContext.stop();
    }
}
