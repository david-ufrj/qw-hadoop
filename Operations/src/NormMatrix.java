/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package operations;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * This software calculate the matrix norm using Apache Hadoop.
 *
 * @version 1.1 19 Jun 2015
 * @author David Souza
 */


public class NormMatrix {

    public static class Map extends
            Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            // "," is the delimiter used in the input file.
            String[] records = line.split(",");
            DoubleWritable output = new DoubleWritable();
            double[] element = new double[2];
            String[] vals;

            // # is the header of the matrix file.
            if (records[0].indexOf("#") == -1) {

                vals = records[3].split("j");
                element[0] = Double.parseDouble(vals[0]);
                element[1] = Double.parseDouble(vals[1]);
                output.set(element[0] * element[0] + element[1] * element[1]);

                context.write(new LongWritable(0), output);

            }
        }

    }


    public static class Combine extends Reducer<LongWritable, DoubleWritable,
            LongWritable, DoubleWritable> {

        public void reduce(LongWritable key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {

            DoubleWritable output = new DoubleWritable();
            double sum = 0.0d;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            output.set(sum);

            context.write(key, output);

        }
    }


    public static class Reduce extends Reducer<LongWritable, DoubleWritable,
            Text, Text> {
        public void reduce(LongWritable key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {

            double sum = 0.0d;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            context.write(null, new Text(Double.toString(Math.sqrt(sum))));

        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path inputPath;
        Path outputPath;
        FileSystem  fs;
        Job job;

        try {

            inputPath = new Path(args[0]);
            outputPath = new Path(args[1]);

            fs = FileSystem.get(new URI(outputPath.toString()), conf);

            // Delete the output directory if it already exists.
            fs.delete(outputPath, true);
            fs.close();

            // Create job
            job = new Job(conf, "MatrixNorm");
            job.setJarByClass(NormMatrix.class);

            // Specify key / value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Setup MapReduce job
            job.setMapperClass(Map.class);
            job.setCombinerClass(Combine.class);
            job.setReducerClass(Reduce.class);

            // Set only the number of reduces tasks
            job.setNumReduceTasks(1);

            // Set Map output Key/Value type
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Input
            FileInputFormat.addInputPath(job, inputPath);

            // Output
            FileOutputFormat.setOutputPath(job, outputPath);

            // Execute job
            job.waitForCompletion(true);


        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
