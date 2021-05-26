package com.readbiomed.spark.uima;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UDTRegistration;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CAS;
import org.apache.uima.fit.factory.JCasFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UIMACasTypeTest {

    @org.junit.jupiter.api.Test
    void sqlType() {
    }

    @org.junit.jupiter.api.Test
    void serialize() {
    }

    @org.junit.jupiter.api.Test
    void deserialize() {
    }

    @org.junit.jupiter.api.Test
    void userClass() {
    }

    @org.junit.jupiter.api.Test
    public void createUDT() {
        // If City wouldn't be annotated with @SQLUserDefinedType, below
        // use of UDT registry should be enough to make it work
        // UDTRegistration.register("com.waitingforcode.sql.udt.City", "com.waitingforcode.sql.udt.CityUserDefinedType");
        SparkSession spark = SparkSession.builder()
                .appName( "Lookup pathogen in taxonomy" )
                .master( "local[*]" )
                .getOrCreate();
        try (JavaSparkContext closeableContext = new JavaSparkContext(spark.sparkContext())) {
            UDTRegistration.register("org.apache.uima.cas.CAS", "com.readbiomed.spark.uima.UIMACasType");
            CAS paris = JCasFactory.createText("testing some creation").getCas();

            List<CAS> cities = Lists.newArrayList(paris);
            JavaRDD<Row> citiesRdd = closeableContext.parallelize(cities)
                    .map(RowFactory::create);
            citiesRdd.collect();

//            StructType citySchema = new StructType().add("uimacas",
//                    new UIMACasType(), false);
//            Dataset<Row> datasetFromRdd = spark.createDataFrame(citiesRdd, citySchema);
//            datasetFromRdd.show(false);

        } catch (UIMAException e) {
            e.printStackTrace();
        }
    }
}