package mypackage;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StringToListFunction implements MapFunction<String, List<Integer>> {
    @Override
    public List<Integer> map(String s) throws Exception {
        List<Integer> l = Arrays.stream(s.split(" "))
                .map(element -> Integer.parseInt(element))
                .collect(Collectors.toList());

        return l;
    }
}
