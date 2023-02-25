package mypackage;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

public class PruneKeyFunction implements MapFunction<List<Integer>, String> {
    @Override
    public String map(List<Integer> service) throws Exception {
        int size = service.size();
        service.remove(size-1);
        return service.toString();
    }
}
