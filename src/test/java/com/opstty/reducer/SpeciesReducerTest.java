package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private SpeciesReducer speciesReducer;

    @Before
    public void setup() {
        this.speciesReducer = new SpeciesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        // Load a sample from the CSV file using a relative path
        URL resource = getClass().getResource("/data/trees.csv");
        BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()));
        String value1 = reader.readLine(); // Skip the header
        String value2 = reader.readLine();
        String value3 = reader.readLine();
        reader.close();

        // Extract the species information
        String[] fields2 = value2.split(";");
        String[] fields3 = value3.split(";");

        List<NullWritable> values = new ArrayList<>();
        values.add(NullWritable.get());

        // Test reducing for the first species
        Text key2 = new Text(fields2[2] + " " + fields2[3]);
        this.speciesReducer.reduce(key2, values, this.context);
        verify(this.context, times(1)).write(key2, NullWritable.get());

        // Test reducing for the second species
        Text key3 = new Text(fields3[2] + " " + fields3[3]);
        this.speciesReducer.reduce(key3, values, this.context);
        verify(this.context, times(1)).write(key3, NullWritable.get());
    }
}
