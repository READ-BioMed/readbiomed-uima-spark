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

public class UIMAModel extends Transformer {

    private Map<String, Class> outColTypeClazzMap;
    private String textCol;

    private static AnalysisEngine analysisEngine;

    private static UIMAModel uimaModel = null;

    private UIMAModel (){

    }

    public static UIMAModel getInstance (Map<String, Class> types, AnalysisEngineDescription[] analysisEngineDescriptions, String textCol) throws Exception {
        if (uimaModel ==null){
            uimaModel = new UIMAModel();
            uimaModel.setTypes(types);
            uimaModel.setTextCol(textCol);
            analysisEngine = getNewAnalysisEngine(analysisEngineDescriptions);

        }

        return uimaModel;
    }

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
            analysisEngine.process(jCas);
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



    public  static AnalysisEngine getNewAnalysisEngine(AnalysisEngineDescription[] analysisEngineDescriptions) throws Exception{

        if (analysisEngine == null){
            AggregateBuilder aggregateBuilder = new AggregateBuilder();

            for (AnalysisEngineDescription analysisEngineDescription : analysisEngineDescriptions){
                aggregateBuilder.add(analysisEngineDescription);
            }
            analysisEngine = aggregateBuilder.createAggregate();
        }

        return analysisEngine;

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
