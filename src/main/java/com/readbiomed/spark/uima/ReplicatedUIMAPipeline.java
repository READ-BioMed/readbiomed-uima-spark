package com.readbiomed.spark.uima;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.resource.ResourceInitializationException;

import java.util.*;

public  class ReplicatedUIMAPipeline extends UIMAPipeline {

    private AnalysisEngineDescription[] descriptions;


    public ReplicatedUIMAPipeline(){
        super();
    }

    public void setDescriptions(AnalysisEngineDescription[] descriptions) {
        this.descriptions = descriptions;
    }


    public  AnalysisEngine getAnalysisEngine(){
        return buildAnalysisEngine();
    }

    public  AnalysisEngine buildAnalysisEngine()  {
        AggregateBuilder aggregateBuilder = new AggregateBuilder();

        for (AnalysisEngineDescription analysisEngineDescription: descriptions){
            aggregateBuilder.add(analysisEngineDescription);
        }
        try {
            return aggregateBuilder.createAggregate();
        } catch (ResourceInitializationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public String uid() {
        return UUID.randomUUID().toString();
    }

}
