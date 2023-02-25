package mypackage;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.List;

public class TriggerKeySelector implements KeySelector<List<Integer>, String> {


    private String algorithm;
    private int numOfPartitions;


    public TriggerKeySelector(String alg, int nPart){
        this.algorithm = alg;
        this.numOfPartitions = nPart;
    }



    @Override
    public String getKey(List<Integer> trigger) throws Exception {

        return generateKey();

    }

    public String generateKey(){
        return generateDimKey();
    }

    public String generateDimKey(){

        String key;

        key = Integer.toString(0);


        return key;

    }
}
