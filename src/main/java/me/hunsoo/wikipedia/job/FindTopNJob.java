package me.hunsoo.wikipedia.job;

import me.hunsoo.wikipedia.PageRank;
import me.hunsoo.wikipedia.mapper.TopNMapper;
import me.hunsoo.wikipedia.reducer.TopNReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FindTopNJob {
    /**
     * Chooses top N pages with higher PageRanks.
     * @param inputPath Input file path which is from the final iteration of pagerank
     * @param outputPath Final output file path
     * @param cachePath
     * @param N How many top results you want on the final output, which comes from command arguments
     * @throws java.io.IOException
     */
    public void queryTopNPageRanks(String inputPath, String outputPath, String cachePath, int N)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Top N Page Ranks");

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // pass N as a configuration parameter
        job.getConfiguration().set("N", Integer.toString(N));

        // add distributed cache
        job.addCacheArchive(new Path(cachePath).toUri());

        // for this specific job, we must have single reducer to finalize top N
        job.setNumReduceTasks(1);

        job.setJarByClass(PageRank.class);
        job.waitForCompletion(true); // set parameter to true for verbose progress logs
    }
}
