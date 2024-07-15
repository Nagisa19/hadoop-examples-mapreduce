package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class MostTreesReducerTest {
    @Mock
    private Reducer.Context context;
    private MostTreesReducer mostTreesReducer;

    @Before
    public void setup() {
        this.mostTreesReducer = new MostTreesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        // Prepare test data
        Text value1 = new Text("7\t3");
        Text value2 = new Text("8\t5");
        Text value3 = new Text("9\t2");

        Iterable<Text> values = Arrays.asList(value1, value2, value3);

        // Run the reduce method
        this.mostTreesReducer.reduce(NullWritable.get(), values, this.context);

        // Verify that context.write() is called with the correct arguments
        verify(this.context, times(1)).write(new Text("8"), new IntWritable(5));
    }
}
