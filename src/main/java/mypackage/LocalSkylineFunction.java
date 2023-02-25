package mypackage;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

public class LocalSkylineFunction extends KeyedCoProcessFunction<Integer, List<Integer>, List<Integer>, List<Integer>> {

    private int dimensions;
    private transient ListState<List<Integer>> skylineState;
    private String algorithm;


    public LocalSkylineFunction(int dims, String alg){
        this.dimensions = dims;
        this.algorithm = alg;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<List<Integer>> skylineStateDescriptor = new ListStateDescriptor<>(
                "skyline",
                TypeInformation.of(new TypeHint<List<Integer>>(){})
        );

        skylineState = getRuntimeContext().getListState(skylineStateDescriptor);

    }


    @Override
    public void processElement1(List<Integer> service, Context context, Collector<List<Integer>> collector) throws Exception {

        boolean dominated = false;
        Iterator<List<Integer>> iter = skylineState.get().iterator();

        if(service.get(0).intValue() == -2) // dominated partition, to be skipped
            return;

        while(iter.hasNext()){
            List<Integer> p = iter.next();

            //if new service dominates older service -> remove older service
            if(dominates(service, p, this.dimensions)){
                iter.remove();
                continue;
            }
            //if older service dominates new service -> new service is not part of the skyline...next one
            if(dominates(p, service, this.dimensions)){
                dominated = true;
                break;
            }
            //if service already in state no reason to keep itearting. It won't knock out any older services
            if(isEqual(service, p, this.dimensions)){
                dominated = true;
                break;
            }

        }
        //compare with everyone and not dominated -> possible skyline service!
        if(!dominated){
            skylineState.add(service);
        }
    }

    @Override
    public void processElement2(List<Integer> trigger, Context context, Collector<List<Integer>> collector) throws Exception {
        Iterable<List<Integer>> elements = skylineState.get();
        for(List<Integer> element: elements) {
            collector.collect(element);
        }
        collector.collect(trigger);
        skylineState.clear();

    }


    public boolean dominates(List<Integer> s1, List<Integer> s2, int dimensions){

        boolean isDominant = false;
        for(int i=0; i<dimensions; i++){

            if(s1.get(i).intValue() > s2.get(i).intValue())
                return false;

            if(s1.get(i).intValue() < s2.get(i).intValue()){
                isDominant = true;
            }
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
