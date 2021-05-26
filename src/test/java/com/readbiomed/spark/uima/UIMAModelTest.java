package com.readbiomed.spark.uima;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UDTRegistration;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;
import scala.reflect.ClassTag;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class UIMAModelTest {

    private SparkSession sparkSession;
    @BeforeEach
    void setUp() {
        sparkSession = SparkSession.builder()
                .appName( "Lookup pathogen in taxonomy" )
                .master( "local[*]" )
                .getOrCreate();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void setAnalysisEngine() {
    }

    @Test
    void transformSchema() {
    }

    @Test
    void transform() throws ResourceInitializationException {





//        sparkSession.sparkContext().broadcast(analysisEngine,classTag(AnalysisEngine.class));

        UIMAModel uimaModel = new UIMAModel();

        Map<String,Class> classMap = new HashMap<>();
        String[] annotationList = {"duplicate_text"};
        classMap.put(annotationList[0],org.apache.uima.jcas.tcas.DocumentAnnotation.class);
        uimaModel.setTypes(classMap);

        // Prepare test documents, which are unlabeled.
        Dataset<Row> data = buildTestDocuments();
        data = uimaModel.transform(data);

        data.collect();
        Iterator<Row> iterator = data.toLocalIterator();
        while (iterator.hasNext()){
            Row row = iterator.next();
            String text = row.<String>getAs("text");
            String[] duplicate_text = (String[])row.getAs(annotationList[0]);
            assertEquals(text,duplicate_text[0]);
        }




    }

    @Test
    void copy() {
    }

    @Test
    void uid() {
    }

    public Dataset<Row> buildTestDocuments(){
        StructType trainingSchema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("text", DataTypes.StringType, false),
                DataTypes.createStructField("label", DataTypes.DoubleType, false)
        ));
        Dataset<Row> training = sparkSession.createDataFrame(Lists.newArrayList(
                RowFactory.create(0L, "a b c d e spark", 1.0),
                RowFactory.create(1L, "b d", 0.0),
                RowFactory.create(2L, "spark f g h", 1.0),
                RowFactory.create(3L, "hadoop mapreduce", 0.0)
        ), trainingSchema);

        return training;
    }

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }
}