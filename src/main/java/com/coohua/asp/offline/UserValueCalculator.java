package com.coohua.asp.offline;

import com.coohua.asp.offline.bean.AdCountItem;
import com.coohua.asp.offline.config.AppConfig;
import com.coohua.asp.offline.utils.LogParserUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


/**
 * Created by liubin on 2016/4/6.
 */
public class UserValueCalculator {

    public static JavaPairRDD<String, Double> calcUserValue(JavaRDD<AdCountItem> adCountItemJavaRDD) {
        final AppConfig appConfig = AppConfig.INSTANCE;
        return adCountItemJavaRDD.mapToPair(new PairFunction<AdCountItem, String, Double>() {
            @Override
            public Tuple2<String, Double> call(AdCountItem v1) throws Exception {
                double sum = v1.getCpaLeft() * appConfig.getUserValueCpaWeight() +
                        v1.getCpeLeft() * appConfig.getUserValueCpeWeight() +
                        v1.getCpmLeft() * appConfig.getUserValueCpmWeight() +
                        v1.getShareLeft() * appConfig.getUserValueShareWeight();
                return new Tuple2<String, Double>(v1.getCoohuaId(), sum);
            }
        });
    }

    public static void processAndSave(JavaRDD<AdCountItem> adCountItemJavaRDD, String day) {
        AppConfig appConfig = AppConfig.INSTANCE;
        JavaPairRDD<String, Double> javaRDD = calcUserValue(adCountItemJavaRDD);
        JavaRDD<String> resultRdd = javaRDD.map(new Function<Tuple2<String, Double>, String>() {
            @Override
            public String call(Tuple2<String, Double> v1) throws Exception {
                return v1._1() + "\t" + v1._2();
            }
        });
        resultRdd.saveAsTextFile(appConfig.getUserValueOutputPath() + "/" + day);
    }

    public static JavaPairRDD<String, Double> processAndGetPairValue(JavaRDD<AdCountItem> adCountItemJavaRDD, String day) {
        JavaPairRDD<String, Double> javaRDD = calcUserValue(adCountItemJavaRDD);
        return javaRDD;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp xx.jar com.coohua.asp.offline.UserValueCalculator [day]");
            System.exit(-1);
        }
        String day = args[0];
        AppConfig appConfig = AppConfig.INSTANCE;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("UserValueCalculator");
        sparkConf.setMaster(appConfig.getSparkUrl());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> file = sparkContext.textFile(appConfig.getUserAdClickCountPath() + "/" + day, 5);
        JavaRDD<AdCountItem> adCountItemJavaRDD = file.map(new Function<String, AdCountItem>() {
            @Override
            public AdCountItem call(String v1) throws Exception {
                return LogParserUtils.parseAdCountItem(v1);
            }
        });
        UserValueCalculator.processAndSave(adCountItemJavaRDD, day);
        sparkContext.stop();
    }
}
