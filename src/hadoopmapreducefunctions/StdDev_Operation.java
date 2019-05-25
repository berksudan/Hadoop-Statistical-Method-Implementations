package hadoopmapreducefunctions;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StdDev_Operation {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);

		@SuppressWarnings("deprecation")
		Job j = new Job(c, "wordcount");
		j.setJarByClass(StdDev_Operation.class);
		j.setMapperClass(MapForStdDevOperation.class);
		j.setReducerClass(ReduceForStdDevOperation.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForStdDevOperation extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final int numOfCols = 109;
		private final int yearIdx = 2, agesStartIdx = 5;

		private String[] lineToList(String line, int num_of_cols) {
			String[] list = new String[num_of_cols];

			line = line.substring(0, line.length() - 1);
			list = line.split(",");

			return list;
		}

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

			String[] rowList = lineToList(value.toString(), numOfCols);
			String year = rowList[yearIdx];
			for (int age = 0; age <= 100; age++) {
				String formattedAge = String.format("%03d", age);
				Integer population = new Integer(Integer.parseInt(rowList[agesStartIdx + age]));

				Text outputKey = new Text(year + ":" + formattedAge);
				IntWritable outputValue = new IntWritable(population);
				con.write(outputKey, outputValue);
//				System.out.println(">>>>>>" + outputKey + "," + outputValue + "written!");
			}
		}

	}

	public static class ReduceForStdDevOperation extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text yearAge, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			List<Integer> valueList = new ArrayList<>();
			int sum = 0, count = 0, mean = 0;
			int sumOfSquares = 0;
			int stdDev = 0;

			for (IntWritable value : values) {
				sum += value.get();
				count++;
				valueList.add(value.get());
			}
			mean = sum / count;
			for (int value : valueList) {
				sumOfSquares += (value - mean) * (value - mean);
			}
			if(count == 1)
				count++;
			
			stdDev = (int) Math.sqrt(sumOfSquares / (count - 1));
			con.write(yearAge, new IntWritable(stdDev));
		}
	}
}
