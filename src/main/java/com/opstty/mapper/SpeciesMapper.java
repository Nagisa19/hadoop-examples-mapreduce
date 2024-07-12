package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final int GENRE_INDEX = 2;
    private static final int SPECIES_INDEX = 3;
    private boolean isFirstLine = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the header
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > SPECIES_INDEX) {
            String fullSpecies = fields[GENRE_INDEX] + " " + fields[SPECIES_INDEX];
            context.write(new Text(fullSpecies), NullWritable.get());
        }
    }
}
