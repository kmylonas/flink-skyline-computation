package mypackage;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.List;

public class DimKeySelector implements KeySelector<List<Integer>, Integer> {

    private String algorithm;
    private int numOfPartitions;
    private int maxValue;
    private int dimensions;
    public DimKeySelector(String alg, int nPart, int mv, int dims){
        this.algorithm = alg;
        this.numOfPartitions = nPart;
        this.maxValue = mv;
        this.dimensions = dims;

    }

    @Override
    public Integer getKey(List<Integer> service) throws Exception {

        Integer key = service.get(dimensions);
        return key;

    }

}



