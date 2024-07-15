package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TallestTreeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxHeight = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
            maxHeight = Math.max(maxHeight, value.get());
        }
        context.write(key, new DoubleWritable(maxHeight));
    }
}
