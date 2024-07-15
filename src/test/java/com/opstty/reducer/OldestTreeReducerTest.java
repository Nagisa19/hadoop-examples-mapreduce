package com.opstty.reducer;

import com.opstty.writable.TreeInfoWritable;
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
public class OldestTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private OldestTreeReducer oldestTreeReducer;

    @Before
    public void setup() {
        this.oldestTreeReducer = new OldestTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        // Prepare test data
        TreeInfoWritable tree1 = new TreeInfoWritable(new IntWritable(100), new Text("District1"));
        TreeInfoWritable tree2 = new TreeInfoWritable(new IntWritable(150), new Text("District2"));
        TreeInfoWritable tree3 = new TreeInfoWritable(new IntWritable(120), new Text("District3"));

        Iterable<TreeInfoWritable> values = Arrays.asList(tree1, tree2, tree3);

        // Run the reduce method
        this.oldestTreeReducer.reduce(new Text("oldestTree"), values, this.context);

        // Verify that context.write() is called with the correct arguments
        verify(this.context, times(1)).write(new Text("Oldest tree district"), new Text("District2"));
    }
}
