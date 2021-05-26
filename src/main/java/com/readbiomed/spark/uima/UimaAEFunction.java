package com.readbiomed.spark.uima;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;

import java.util.List;

public class UimaAEFunction implements UDF1<String, List> {

    private String colOutput;
    private Class typeClass;
    private AnalysisEngine analysisEngine;

    public UimaAEFunction(String colOutput, Class typeClass, AnalysisEngine analysisEngine){
        this.colOutput = colOutput;
        this.typeClass = typeClass;
        this.analysisEngine = analysisEngine;

    }

    @Override
    public List call(String text) throws Exception {
        String textValue = text;
        JCas jCas = JCasFactory.createText(textValue);
        analysisEngine.process(jCas);
        return JCasUtil.toText(JCasUtil.select(jCas,typeClass));
    }
}
