package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MostTreesMapperTest {
    @Mock
    private Mapper.Context context;
    private MostTreesMapper mostTreesMapper;

    @Before
    public void setup() {
        this.mostTreesMapper = new MostTreesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value1 = "7\t3";
        String value2 = "8\t5";

        // First data line
        this.mostTreesMapper.map(null, new Text(value1), this.context);
        verify(this.context, times(1)).write(NullWritable.get(), new Text(value1));

        // Second data line
        this.mostTreesMapper.map(null, new Text(value2), this.context);
        verify(this.context, times(1)).write(NullWritable.get(), new Text(value2));
    }
}
