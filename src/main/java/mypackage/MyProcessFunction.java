package mypackage;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

public class MyProcessFunction extends KeyedProcessFunction<Integer, List<Integer>, List<Integer>> {

    private int dimensions;

    public MyProcessFunction(int dims){
        this.dimensions = dims;
    }

    private transient ListState<List<Integer>> skylineState;

    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<List<Integer>> skylineStateDescriptor = new ListStateDescriptor<>(
                "skyline",
                TypeInformation.of(new TypeHint<List<Integer>>(){})
        );

        skylineState = getRuntimeContext().getListState(skylineStateDescriptor);

    }

    @Override
    public void processElement(List<Integer> service, Context context, Collector<List<Integer>> collector) throws Exception {

        boolean dominated = false;
        Iterator<List<Integer>> iter = skylineState.get().iterator();

        while(iter.hasNext()){
            List<Integer> p = iter.next();
            if(dominates(service, p, this.dimensions)){
                iter.remove();
            }
            if(dominates(p, service,this.dimensions)){
                dominated = true;
                break;
            }
        }
        if(!dominated){
            skylineState.add(service);
            collector.collect(service);
        }


    }


    public static boolean dominates(List<Integer> s1, List<Integer> s2, int dimensions){

        boolean isDominant = false;
        for(int i=0; i<dimensions; i++){

            if(s1.get(i) > s2.get(i))
                return false;

            if(s1.get(i) < s2.get(i))
                isDominant = true;


        }
        return isDominant;
    }
}
