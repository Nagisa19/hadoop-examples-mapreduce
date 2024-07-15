package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TallestTreeMapperTest {
    @Mock
    private Mapper.Context context;
    private TallestTreeMapper tallestTreeMapper;

    @Before
    public void setup() {
        this.tallestTreeMapper = new TallestTreeMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        // Load a sample from the CSV file using a relative path
        URL resource = getClass().getResource("/data/trees.csv");
        BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()));
        String header = reader.readLine();
        String value1 = reader.readLine();
        String value2 = reader.readLine();
        reader.close();

        // Simulate reading the header line
        this.tallestTreeMapper.map(null, new Text(header), this.context);

        // First data line
        this.tallestTreeMapper.map(null, new Text(value1), this.context);
        String[] fields1 = value1.split(";");
        String genre1 = fields1[2];
        double height1 = Double.parseDouble(fields1[6]);
        verify(this.context, times(1)).write(new Text(genre1), new DoubleWritable(height1));

        // Second data line
        this.tallestTreeMapper.map(null, new Text(value2), this.context);
        String[] fields2 = value2.split(";");
        String genre2 = fields2[2];
        double height2 = Double.parseDouble(fields2[6]);
        verify(this.context, times(1)).write(new Text(genre2), new DoubleWritable(height2));
    }
}
