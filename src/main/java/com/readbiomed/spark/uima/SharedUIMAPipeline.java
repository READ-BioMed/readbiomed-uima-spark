package com.readbiomed.spark.uima;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AggregateBuilder;

import java.util.Map;

/***
 * A class to represent a shared uima pipeline between all different instances of a uima model.
 * Use this if you want to create an expensive pipeline or a pipeline.
 *
 * The shared pipeline is represented by the <b>analysisEngine</b> static field.
 */
public class SharedUIMAPipeline extends UIMAPipeline {



    private  static  AnalysisEngine analysisEngine;

    private SharedUIMAPipeline(){

    }




    public static UIMAPipeline getNewInstance(Map<String, Class> types, AnalysisEngineDescription[] analysisEngineDescriptions, String textCol) throws Exception {

        buildAnalysisEngine(analysisEngineDescriptions);

        SharedUIMAPipeline sharedUIMAPipelineModel = new SharedUIMAPipeline();
        sharedUIMAPipelineModel.setTypes(types);
        sharedUIMAPipelineModel.setTextCol(textCol);

        return sharedUIMAPipelineModel;
    }

    @Override
    public synchronized AnalysisEngine getAnalysisEngine() {
        return  analysisEngine;
    }

    public  synchronized static void buildAnalysisEngine(AnalysisEngineDescription[] analysisEngineDescriptions) throws Exception{
        if (analysisEngine == null){
            AggregateBuilder aggregateBuilder = new AggregateBuilder();

            for (AnalysisEngineDescription analysisEngineDescription : analysisEngineDescriptions){
                aggregateBuilder.add(analysisEngineDescription);
            }
            analysisEngine = aggregateBuilder.createAggregate();
        }
    }
}
