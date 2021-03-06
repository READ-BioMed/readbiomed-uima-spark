package com.readbiomed.spark.uima;

import com.google.common.collect.Lists;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SharedUIMAPipelineTest {

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
    void transform() throws Exception {
        Map<String,Class> classMap = new HashMap<>();
        String[] annotationList = {"duplicate_text"};
        classMap.put(annotationList[0],org.apache.uima.jcas.tcas.DocumentAnnotation.class);
        UIMAPipeline uimaModel = SharedUIMAPipeline.getNewInstance(classMap, new AnalysisEngineDescription[0],"text" );



        // Prepare test documents, which are unlabeled.
        Dataset<Row> data = buildTestDocuments();
        data = uimaModel.transform(data);

        data.collect();
        Iterator<Row> iterator = data.toLocalIterator();
        while (iterator.hasNext()){
            Row row = iterator.next();
            String text = row.<String>getAs("text");
            ;
            assertEquals(text,JavaConverters.asJavaIterable(row.getAs(annotationList[0])).iterator().next());
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