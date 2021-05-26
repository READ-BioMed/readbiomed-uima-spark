package com.readbiomed.spark.uima;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UimaExractionMap implements MapFunction<Row, Row> {

    private Map<String,Class> outColTypeClazzMap;
    private AnalysisEngine analysisEngine;

    public UimaExractionMap(Map<String,Class> outColTypeClazzMap, AnalysisEngine analysisEngine){
        this.outColTypeClazzMap = outColTypeClazzMap;
        this.analysisEngine = analysisEngine;

    }


    @Override
    public Row call(Row row) throws Exception {
        String textValue = row.<String>getAs("text").toString();
        final JCas jCas = JCasFactory.createText(textValue);
        analysisEngine.process(jCas);

        StructType structType = new StructType();
        List<Object> values = new ArrayList<>();

        StructField[] structFields = row.schema().fields();
        for (int i = 0; i < structFields.length; i++){
            structType.add(structFields[i]);
            values.add(row.get(i));
        }

        outColTypeClazzMap.keySet().stream().forEach(annotation->structType.add(annotation, DataTypes.createArrayType(DataTypes.StringType)));
        outColTypeClazzMap.entrySet().stream().forEach(entry->values.add(JCasUtil.toText(JCasUtil.select(jCas,entry.getValue()))));

        return new GenericRowWithSchema(values.toArray(),structType);

    }
}
