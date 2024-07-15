package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreeCountReducerTest {
    @Mock
    private Reducer.Context context;
    private TreeCountReducer treeCountReducer;

    @Before
    public void setup() {
        this.treeCountReducer = new TreeCountReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        // Prepare test data
        Iterable<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1));

        // Run the reduce method
        this.treeCountReducer.reduce(new Text("7"), values, this.context);

        // Verify that context.write() is called with the correct arguments
        verify(this.context, times(1)).write(new Text("7"), new IntWritable(3));
    }
}
