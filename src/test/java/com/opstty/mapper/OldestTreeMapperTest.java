package com.opstty.mapper;

import com.opstty.writable.TreeInfoWritable;
import org.apache.hadoop.io.IntWritable;
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
public class OldestTreeMapperTest {
    @Mock
    private Mapper.Context context;
    private OldestTreeMapper oldestTreeMapper;

    @Before
    public void setup() {
        this.oldestTreeMapper = new OldestTreeMapper();
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
        this.oldestTreeMapper.map(null, new Text(header), this.context);

        // First data line
        this.oldestTreeMapper.map(null, new Text(value1), this.context);
        String[] fields1 = value1.split(";");
        int yearPlanted1 = Integer.parseInt(fields1[5]);
        int age1 = 2024 - yearPlanted1;
        String district1 = fields1[1];
        verify(this.context, times(1)).write(new Text("oldestTree"), new TreeInfoWritable(new IntWritable(age1), new Text(district1)));

        // Second data line
        this.oldestTreeMapper.map(null, new Text(value2), this.context);
        String[] fields2 = value2.split(";");
        int yearPlanted2 = Integer.parseInt(fields2[5]);
        int age2 = 2024 - yearPlanted2;
        String district2 = fields2[1];
        verify(this.context, times(1)).write(new Text("oldestTree"), new TreeInfoWritable(new IntWritable(age2), new Text(district2)));
    }
}
