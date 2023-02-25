package mypackage;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class HashFunction implements MapFunction<List<Integer>, List<Integer>> {

    private int dimensions;
    private int partitions;
    private int maxValue;
    private String algorithm;
    private int totalNumOfPartitions;

    public HashFunction(int dims, int nop, int mv, String alg){
        this.partitions = nop;
        this.dimensions = dims;
        this.maxValue = mv;
        this.algorithm = alg;
        this.totalNumOfPartitions = (int)Math.pow(partitions, dimensions); //only used in grid case
    }

    @Override
    public List<Integer> map(List<Integer> service) throws Exception {

        List<Integer> l = hashFunction(service);
        return l;
    }

    public List<Integer> hashFunction(List<Integer> service){

        int range = 0;
        int key=0;
        Integer xi;

        switch (algorithm){
            case "dim":
                range = this.maxValue / this.partitions;
                xi = service.get(0)/range;
                if(xi == partitions) //in case of float range make sure that biggest values end up at the last valid partition
                    xi--;
                key=xi;
                service.add(key);
                return service;
            case "grid":
                range = this.maxValue / this.partitions;
                int nonZeros = 0;
                for(int i=1; i<=dimensions; i++){
                    xi = service.get(i-1)/range;
                    if(xi == totalNumOfPartitions) //in case of float range make sure that biggest values end up at the last valid partition
                        xi--;
                    if(xi!=0)
                        nonZeros++;
                    key = key + ((int)Math.pow(partitions, dimensions-i)*(xi));
                    }
                if(nonZeros == dimensions){ //should be skipped
                    service.set(0, -2);
                }
                service.add(key);
                return service;
            case "angle":
                double angleRange = ((Math.PI/2) / this.partitions);

                List<Double> sphCoords = getSphericalCoordinates(service);

                for(int i=0; i<sphCoords.size(); i++){
                    xi = (int)(sphCoords.get(i) / angleRange);
                    key = key + ((int)Math.pow(partitions, dimensions-(i+2))*(xi));
                }

                service.add(key);
                return service;
        }
        return service;

    }


    public List<Double> getSphericalCoordinates(List<Integer> service){
        List<Double> sph = new ArrayList<>(); //n-1 size
        double sum;
        for(int i=0; i<dimensions-1; i++){
            sum = 0;
            for(int j=i+1; j<dimensions; j++){
                sum = sum + Math.pow(service.get(j), 2);
            }

            sph.add(Math.atan( Math.sqrt(sum) / service.get(i) ));
        }
        return sph;
    }

}


//case "angle":
//        double angleRange = ((Math.PI/2) / this.partitions);
//        double serviceAngle = Math.atan(service.get(0)/service.get(1));
//        key = (int)(serviceAngle / angleRange);
//        service.add(key);
//        return service;