package com.readbiomed.spark.uima;

import com.google.common.base.Joiner;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.uima.analysis_engine.AnalysisEngine;
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

    private Map<String,Class> outColTypeClazzMap;
    private String textCol;


    public void setTypes(Map<String, Class> types) {
        this.outColTypeClazzMap = types;
    }

    public void setTextCol(String textCol){
        this.textCol = textCol;
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return schema;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
            Dataset<Row> rowDataset = (Dataset<Row>) dataset;
            rowDataset = rowDataset.map((MapFunction<Row, Row>) row -> {
                final AnalysisEngine analysisEngine = buildAnalysisEngine();
                String textValue = row.<String>getAs(textCol).toString();

                final JCas jCas = JCasFactory.createText(textValue);
                analysisEngine.process(jCas);


                final List<Object> values = new ArrayList<>();

                final ArrayList<StructField> structFields = new ArrayList<>(Arrays.asList(row.schema().fields()));
                for (int i = 0; i < structFields.size(); i++) {
                    values.add(row.get(i));
                }

                outColTypeClazzMap.keySet().stream().forEach(annotation -> {
                    StructField structField =  StructField.apply(annotation, DataTypes.createArrayType(DataTypes.StringType),true,Metadata.empty());
                    structFields.add(structField);
                });
                outColTypeClazzMap.entrySet().stream().forEach(entry -> {
                        values.add(JCasUtil.toText(JCasUtil.select(jCas, entry.getValue())).toArray(new String[1]));
                });

                return new GenericRowWithSchema(values.toArray(), new StructType(structFields.toArray(new StructField[1])));
            }, Encoders.kryo(Row.class));
            return rowDataset;
        }

    protected AnalysisEngine buildAnalysisEngine() throws Exception {
        AggregateBuilder aggregateBuilder = new AggregateBuilder();
        final AnalysisEngine analysisEngine = aggregateBuilder.createAggregate();
        return analysisEngine;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    @Override
    public String uid() {
        return  UUID.randomUUID().toString();
    }
}
