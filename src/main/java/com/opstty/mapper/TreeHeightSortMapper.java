package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private static final int HEIGHT_INDEX = 6;
    private static final int ADDRESS_INDEX = 8;
    private boolean isFirstLine = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > HEIGHT_INDEX) {
            try {
                double height = Double.parseDouble(fields[HEIGHT_INDEX]);
                String address = fields[ADDRESS_INDEX];
                context.write(new DoubleWritable(height), new Text(address));
            } catch (NumberFormatException e) {
                System.out.println("Invalid height: " + fields[HEIGHT_INDEX]);
            }
        }
    }
}
