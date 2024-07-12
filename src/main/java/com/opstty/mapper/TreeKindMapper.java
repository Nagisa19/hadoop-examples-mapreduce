package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeKindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int GENRE_INDEX = 2;
    private boolean isFirstLine = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the header
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > GENRE_INDEX) {
            String genre = fields[GENRE_INDEX];
            context.write(new Text(genre), new IntWritable(1));
        }
    }
}
