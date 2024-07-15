package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int DISTRICT_INDEX = 1;
    private Text district = new Text();
    private boolean isFirstLine = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > DISTRICT_INDEX) {
            district.set(fields[DISTRICT_INDEX]);
            context.write(district, new IntWritable(1));
        }
    }
}
