package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
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
public class TallestTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private TallestTreeReducer tallestTreeReducer;

    @Before
    public void setup() {
        this.tallestTreeReducer = new TallestTreeReducer();
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

        // Extract the height information
        String[] fields2 = value2.split(";");
        String[] fields3 = value3.split(";");

        List<DoubleWritable> values2 = new ArrayList<>();
        values2.add(new DoubleWritable(Double.parseDouble(fields2[6])));

        List<DoubleWritable> values3 = new ArrayList<>();
        values3.add(new DoubleWritable(Double.parseDouble(fields3[6])));

        // Test reducing for the first genre
        Text key2 = new Text(fields2[2]);
        this.tallestTreeReducer.reduce(key2, values2, this.context);
        verify(this.context, times(1)).write(key2, new DoubleWritable(Double.parseDouble(fields2[6])));

        // Test reducing for the second genre
        Text key3 = new Text(fields3[2]);
        this.tallestTreeReducer.reduce(key3, values3, this.context);
        verify(this.context, times(1)).write(key3, new DoubleWritable(Double.parseDouble(fields3[6])));
    }
}
