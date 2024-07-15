package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreesReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    private Text maxDistrict = new Text();
    private int maxCount = 0;

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            String district = fields[0];
            int count = Integer.parseInt(fields[1]);

            if (count > maxCount) {
                maxCount = count;
                maxDistrict.set(district);
            }
        }
        context.write(maxDistrict, new IntWritable(maxCount));
    }
}
