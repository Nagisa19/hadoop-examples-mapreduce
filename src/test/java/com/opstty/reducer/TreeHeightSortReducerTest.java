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
public class TreeHeightSortReducerTest {
    @Mock
    private Reducer.Context context;
    private TreeHeightSortReducer treeHeightSortReducer;

    @Before
    public void setup() {
        this.treeHeightSortReducer = new TreeHeightSortReducer();
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

        // Extract the height and address information
        String[] fields2 = value2.split(";");
        String[] fields3 = value3.split(";");

        List<Text> addresses2 = new ArrayList<>();
        addresses2.add(new Text(fields2[8]));

        List<Text> addresses3 = new ArrayList<>();
        addresses3.add(new Text(fields3[8]));

        // Test reducing for the first height
        DoubleWritable key2 = new DoubleWritable(Double.parseDouble(fields2[6]));
        this.treeHeightSortReducer.reduce(key2, addresses2, this.context);
        verify(this.context, times(1)).write(key2, new Text(fields2[8]));

        // Test reducing for the second height
        DoubleWritable key3 = new DoubleWritable(Double.parseDouble(fields3[6]));
        this.treeHeightSortReducer.reduce(key3, addresses3, this.context);
        verify(this.context, times(1)).write(key3, new Text(fields3[8]));
    }
}
