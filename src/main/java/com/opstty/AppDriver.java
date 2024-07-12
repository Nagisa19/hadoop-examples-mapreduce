package com.opstty;

import com.opstty.job.WordCount;
import com.opstty.job.DistrictsContainingTrees;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String[] argv) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts", DistrictsContainingTrees.class,
                    "A map/reduce program that lists the distinct districts containing trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
