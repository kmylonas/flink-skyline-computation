package mypackage;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;

public class GlobalSkylineCalculator extends KeyedProcessFunction<Integer, List<Integer>, List<Integer>> {

    private static final Logger logger = LogManager.getLogger(GlobalSkylineCalculator.class);
    private int dimensions;
    private int numOfTriggers;
    private String algorithm;

    private transient ListState<List<Integer>> skylineState;
    private transient ValueState<Integer> reachedTriggers;
    private transient ValueState<Long> startTimeState;

    public GlobalSkylineCalculator(int dims, int numOfPartitions, String alg){
        this.dimensions = dims;
        if(alg.equals("dim"))
            this.numOfTriggers = numOfPartitions;
        else if(alg.equals("grid"))
            this.numOfTriggers = (int)Math.pow(numOfPartitions, dims);
        else //angle
            this.numOfTriggers = (int)Math.pow(numOfPartitions, dims-1);
//            this.numOfTriggers = alg.equals("dim") || alg.equals("angle")  ? numOfPartitions : (int)Math.pow(numOfPartitions, dims) ;
    }



    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<List<Integer>> skylineStateDescriptor = new ListStateDescriptor<>(
                "skyline",
                TypeInformation.of(new TypeHint<List<Integer>>(){})
        );

        ValueStateDescriptor<Integer> triggersStateDescriptor = new ValueStateDescriptor<>(
                "trigger-state",
                Integer.class
        );

        ValueStateDescriptor<Long> startTimeDescriptor = new ValueStateDescriptor<>(
                "time-state",
                Long.class
        );

        startTimeState = getRuntimeContext().getState(startTimeDescriptor);
        skylineState = getRuntimeContext().getListState(skylineStateDescriptor);
        reachedTriggers = getRuntimeContext().getState(triggersStateDescriptor);

    }

    @Override
    public void processElement(List<Integer> service, Context context, Collector<List<Integer>> collector) throws Exception {

        boolean dominated = false;
        Iterator<List<Integer>> iter = skylineState.get().iterator();

        if(startTimeState.value() == null){
            Long sTime = context.timerService().currentProcessingTime();
            startTimeState.update(sTime);
        }

        if(reachedTriggers.value()==null)
            reachedTriggers.update(Integer.valueOf(0));

        if(isTrigger(service)){
            reachedTriggers.update(reachedTriggers.value().intValue()+1);

            if(reachedTriggers.value() == numOfTriggers){
                //time to collect
                while(iter.hasNext()){
                    collector.collect(iter.next());
                }

                Long endTime = context.timerService().currentProcessingTime();
                logger.info("Start time: " + startTimeState.value());
                logger.info("End time: " + endTime);
                logger.info("Total time: " + (endTime - startTimeState.value()));

                //                skylineState.clear();
                //reset triggers to 0
                reachedTriggers.clear();
                startTimeState.clear();

            }
        }
        else{
            // calculate skyline
            while(iter.hasNext()){
                List<Integer> p = iter.next();
                if(dominates(service, p, this.dimensions)){
                    iter.remove();
                }
                if(dominates(p, service,this.dimensions)){
                    dominated = true;
                    break;
                }
                //if service already in state no reason to keep itearting. It won't knock out any older services
                if(isEqual(service, p, this.dimensions)){
                    dominated = true;
                    break;
                }
            }
            if(!dominated){
                skylineState.add(service);
            }
        }


    }

    public boolean isTrigger(List<Integer> event){
        return (event.get(0).intValue() == -1);
    }

    public boolean dominates(List<Integer> s1, List<Integer> s2, int dimensions){

        boolean isDominant = false;
        for(int i=0; i<dimensions; i++){

            if(s1.get(i).intValue() > s2.get(i).intValue())
                return false;

            if(s1.get(i).intValue() < s2.get(i).intValue())
                isDominant = true;


        }
        return isDominant;
    }

    public boolean isEqual(List <Integer> s1, List<Integer> s2, int dimensions){

        for(int i=0; i<dimensions; i++){
            if(s1.get(i).intValue()!=s2.get(i).intValue()){
                return false;
            }

        }
        return true;
    }
}



