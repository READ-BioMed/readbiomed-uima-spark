package com.readbiomed.spark.uima;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicatedUIMAPipelineTest {

    private SparkSession sparkSession;
    private StructType trainingSchema;

    @BeforeEach
    void setUp() {
        sparkSession = SparkSession.builder()
                .appName( "Lookup pathogen in taxonomy" )
                .master( "local[*]" )
                .getOrCreate();

        trainingSchema = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("text", DataTypes.StringType, false),
                DataTypes.createStructField("label", DataTypes.DoubleType, false)
        ));
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


        // Prepare test documents, which are unlabeled.
        Dataset<Row> data = buildTestDocuments();


        ReplicatedUIMAPipeline replicatedUIMAModel = new ReplicatedUIMAPipeline();
        for (String textCol: Arrays.asList("title","text")){
            Map<String,Class> classMap = new HashMap<>();
            String[] annotationList = {"duplicate_"+textCol};
            classMap.put(annotationList[0],org.apache.uima.jcas.tcas.DocumentAnnotation.class);

            replicatedUIMAModel.setTypes(classMap);
            replicatedUIMAModel.setTextCol(textCol);

            data = replicatedUIMAModel.transform(data);

            data.collect();
            Iterator<Row> iterator = data.toLocalIterator();
            while (iterator.hasNext()){
                Row row = iterator.next();
                String text = row.<String>getAs(textCol);
                ;
                assertEquals(text,JavaConverters.asJavaIterable(row.getAs(annotationList[0])).iterator().next());
            }
        }


        ArrayList<StructField> newFields = Lists.newArrayList(trainingSchema.fields());
        Arrays.asList("title","text").forEach(textCol->newFields.add(DataTypes.createStructField("duplicate_"+textCol, DataTypes.StringType, false)));

        assertArrayEquals(data.schema().fieldNames(),DataTypes.createStructType(newFields).fieldNames());
    }

    @Test
    void copy() {
    }

    @Test
    void uid() {
    }

    public Dataset<Row> buildTestDocuments(){

        Dataset<Row> training = sparkSession.createDataFrame(Lists.newArrayList(
                RowFactory.create(0L, "Spark UIMA","This is about spark uima integration", 1.0),
                RowFactory.create(1L, "Install Spark","You need to scala to install spark", 0.0),
                RowFactory.create(2L, "UIMA Engine","You can run any UIMA engine", 1.0),
                RowFactory.create(3L, "Spark and Hadoop","hadoop mapreduce", 0.0)
        ), trainingSchema);

        return training;
    }

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }
}