package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final int DISTRICT_INDEX = 1;
    private boolean isFirstLine = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the header
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > DISTRICT_INDEX) {
            context.write(new Text(fields[DISTRICT_INDEX]), NullWritable.get());
        }
    }
}
