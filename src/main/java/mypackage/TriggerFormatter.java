package mypackage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;


//formatter and multiplier
public class TriggerFormatter implements FlatMapFunction<String, List<Integer>> {

    private int numOfPartitions;
    private String algorithm;
    private int maxValue;
    private int dimensions;


    public TriggerFormatter(int nPart, String alg, int maxValue, int dimensions) {
        this.numOfPartitions = nPart;
        this.algorithm = alg;
        this.maxValue = maxValue;
        this.dimensions = dimensions;

    }


    @Override
    public void flatMap(String s, Collector<List<Integer>> collector) throws Exception {

        List<Integer> trigger = new ArrayList<>();
        int totalNumOfPartitions;

        switch (algorithm){
            case "dim":
                for(int i=0; i<numOfPartitions; i++){
                    for(int j=0; j<dimensions; j++){
                        trigger.add(-1);
                    }
                    trigger.add(i);
                    collector.collect(trigger);
                    trigger.clear();
                }
                break;
            case "grid":
                totalNumOfPartitions = (int)Math.pow(numOfPartitions, dimensions);
                for(int i=0; i<totalNumOfPartitions; i++){
                    for(int j=0; j<dimensions; j++){
                        trigger.add(-1);
                    }
                    trigger.add(i);
                    collector.collect(trigger);
                    trigger.clear();
                }
                break;
            case "angle":
                totalNumOfPartitions = (int)Math.pow(numOfPartitions, dimensions-1);
                for(int i=0; i<totalNumOfPartitions; i++) {
                    for (int j = 0; j < dimensions; j++) {
                        trigger.add(-1);
                    }
                    trigger.add(i);
                    collector.collect(trigger);
                    trigger.clear();
                }
                break;
        }
    }
}
