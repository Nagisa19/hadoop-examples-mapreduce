package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TallestTreeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final int GENRE_INDEX = 2;
    private static final int HEIGHT_INDEX = 6;
    private boolean isFirstLine = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the header
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > HEIGHT_INDEX) {
            try {
                String genre = fields[GENRE_INDEX];
                double height = Double.parseDouble(fields[HEIGHT_INDEX]);
                context.write(new Text(genre), new DoubleWritable(height));
            } catch (NumberFormatException e) {
                System.out.println("Invalid height: " + fields[HEIGHT_INDEX]);
            }
        }
    }
}
