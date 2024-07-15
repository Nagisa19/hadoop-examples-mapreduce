package com.opstty.mapper;

import com.opstty.writable.TreeInfoWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, Text, TreeInfoWritable> {
    private static final int DISTRICT_INDEX = 1;
    private static final int YEAR_PLANTED_INDEX = 5;
    private boolean isFirstLine = true;
    private final Text district = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > YEAR_PLANTED_INDEX) {
            try {
                int yearPlanted = Integer.parseInt(fields[YEAR_PLANTED_INDEX]);
                int age = 2024 - yearPlanted; // Assuming current year is 2024
                district.set(fields[DISTRICT_INDEX]);
                context.write(new Text("oldestTree"), new TreeInfoWritable(new IntWritable(age), district));
            } catch (NumberFormatException e) {
                System.out.println("Invalid year: " + fields[YEAR_PLANTED_INDEX]);
            }
        }
    }
}
