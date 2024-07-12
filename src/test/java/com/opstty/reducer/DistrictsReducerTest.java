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
public class DistrictsReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictsReducer districtsReducer;

    @Before
    public void setup() {
        this.districtsReducer = new DistrictsReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        // Load a sample from the CSV file using a relative path
        URL resource = getClass().getResource("/data/trees.csv");
        BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()));
        String value1 = reader.readLine();
        String value2 = reader.readLine();
        reader.close();

        // Extract the district information
        String[] fields1 = value1.split(";");
        String[] fields2 = value2.split(";");

        List<NullWritable> values = new ArrayList<>();
        values.add(NullWritable.get());

        // Test reducing for the first district
        Text key1 = new Text(fields1[1]);
        this.districtsReducer.reduce(key1, values, this.context);
        verify(this.context, times(1)).write(key1, NullWritable.get());

        // Test reducing for the second district
        Text key2 = new Text(fields2[1]);
        this.districtsReducer.reduce(key2, values, this.context);
        verify(this.context, times(1)).write(key2, NullWritable.get());
    }
}
