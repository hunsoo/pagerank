package me.hunsoo.wikipedia;

import me.hunsoo.wikipedia.job.*;
import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Calculates PageRank of articles from a snapshot of English Wikipedia dumps.
 */
public class PageRank {
    private static Logger logger = Logger.getLogger(PageRank.class);
    private static NumberFormat twoDigits = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        final PageRank pageRank = new PageRank();

        if (args.length != 4) {
            System.err.println("Usage: PageRank <input path> <output path> <number of pagerank calculation iterations> <N>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int iterations = Integer.valueOf(args[2]);
        int N = Integer.valueOf(args[3]);

        // stopwatch from apache commons - let's see how long it would take to process the whole thing
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Job 1: parse XML and extract links from all pages
        LinkExtractionJob linkExtractionJob = new LinkExtractionJob();
        linkExtractionJob.extractLinks(inputPath, outputPath + "/links");

        // Job 2: Generate lookup table for page id/title, which would be used as distributed cache
        LookupTableGenerationJob lookupTableGenerationJob = new LookupTableGenerationJob();
        lookupTableGenerationJob.generate(outputPath + "/links", outputPath + "/lookup-table");

        // Job 3: Generate link graph of page ids and remove link pages that cannot be identified with id
        LinkGraphGenerationJob linkGraphGenerationJob = new LinkGraphGenerationJob();
        linkGraphGenerationJob.generate(outputPath + "/links", outputPath + "/iteration-00", outputPath + "/lookup-table/part-r-00000");


        // Job 4: calculate PageRank (iterative)
        PageRankCalculationJob pageRankCalculationJob = new PageRankCalculationJob();
        for (int i = 0; i < iterations; i++) {
            pageRankCalculationJob.calculatePageRank(outputPath + "/iteration-" + twoDigits.format(i), outputPath + "/iteration-" + twoDigits.format(i + 1));
        }

        // Job 5: sort top N by PageRank
        FindTopNJob findTopNJob = new FindTopNJob();
        findTopNJob.queryTopNPageRanks(outputPath + "/iteration-" + twoDigits.format(iterations), outputPath + "/top-" + N + "-pagerank", outputPath + "/lookup-table/part-r-00000", N);

        stopWatch.stop();
        logger.info("Completed Jobs in " + stopWatch.getTime()/1000.0 + " seconds.");
        stopWatch.reset();
    }
}