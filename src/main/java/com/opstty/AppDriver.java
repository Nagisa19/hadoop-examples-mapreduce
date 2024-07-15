package com.opstty;

import com.opstty.job.*;
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
            programDriver.addClass("specieslist", SpeciesList.class,
                    "A map/reduce program that lists the different species of trees.");
            programDriver.addClass("treekindcount", TreeKindCount.class,
                    "A map/reduce program that counts the number of trees of each kind.");
            programDriver.addClass("tallesttree", TallestTree.class,
                    "A map/reduce program that finds the tallest tree of each kind.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
