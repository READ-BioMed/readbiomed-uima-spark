package com.readbiomed.spark.uima;

import com.google.common.base.Joiner;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.TypeClass;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;


import java.util.*;

/**
 * The base UIMAPipeline implementation that does the work of transforming a dataset using a UIMA Pipeline.
 *
 * The pipeline works by running a completed UIMA pipeline as represented by an AnalysisEngine.
 *
 * The pipeline expects a single text column, <b>textCol</b>, as input and outputs the results of the pipeline as a ArrayType of a string type.
 *
 * You can outputs more than a Uima Type as a column through the <b>outColTypeClazzMap</b> map.
 *
 * One major issue with running UIMA pipeline in spark is that the analysis engines are not serializable.
 * To overcome this problem, we delay the process of supplying the AnalysisEngine to run time, through the @getAnalysisEngine abstract method.
 *
 * Both implementations 
 *
 */
public abstract class UIMAPipeline extends Transformer {

    private Map<String, Class> outColTypeClazzMap;
    private String textCol;

    protected UIMAPipeline(){

    };

    /**
     * Return the
     * @return
     */
    public abstract AnalysisEngine getAnalysisEngine();



    public void setTypes(Map<String, Class> types) {
        this.outColTypeClazzMap = types;
    }

    public void setTextCol(String textCol) {
        this.textCol = textCol;
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return schema;
    }

    @Override
    public Dataset transform(Dataset<?> dataset) {
        Dataset rowDataset = (Dataset<Row>) dataset;

        StructType schema = buildSchema(rowDataset.schema());

        ExpressionEncoder<Row> rowEncoder =  RowEncoder.apply(schema);;

        rowDataset = rowDataset.map( new MapFunction<Row, GenericRow>() {
            @Override
            public GenericRow call(Row row) throws Exception {
                return populateUIMAFields(schema,row);
            }
        },rowEncoder);
        rowDataset.show();

        return rowDataset;
    }

    private StructType buildSchema(StructType schema) {
        final ArrayList<StructField> structFields = new ArrayList<>(Arrays.asList(schema.fields()));

        outColTypeClazzMap.keySet().stream().forEach(annotation -> {
            StructField structField = StructField.apply(annotation, DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty());
            structFields.add(structField);
        });


       return new StructType(structFields.toArray(new StructField[structFields.size()]));
    }

    private GenericRowWithSchema populateUIMAFields(StructType schema, Row row) throws UIMAException {
        String textValue = row.<String>getAs(textCol).toString();
        final JCas jCas = JCasFactory.createText(textValue);;
        try {
            getAnalysisEngine().process(jCas);
        }catch (Exception e){
            System.out.println("Failed to process " + textValue);
        }

        final List<Object> values = new ArrayList<>();

        StructField[] rowStructedFields = row.schema().fields();

        for (int i = 0; i < rowStructedFields.length; i++) {
            values.add(row.getAs(rowStructedFields[i].name()));
        }


        outColTypeClazzMap.entrySet().stream().forEach(entry -> {
            values.add(JCasUtil.toText(JCasUtil.select(jCas, entry.getValue())).toArray(new String[1]));
        });

        return new GenericRowWithSchema(values.toArray(), schema);
    }






    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    @Override
    public String uid() {
        return UUID.randomUUID().toString();
    }

}
