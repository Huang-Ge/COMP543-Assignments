
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RevenueCount extends Configured implements Tool {

    static int printUsage() {
        System.out.println("RevenueCount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class RevenueMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        // so we don't have to do reallocations
        //private final static DoubleWritable totalRev = new DoubleWritable(1);
        //private Text curDate = new Text();

        // to check for
        String doubleExpression = "\\d{1,10}(\\.\\d{1,10})?";
        String driverIdExpression = "([a-zA-Z]+\\d|\\d+[a-zA-Z])\\w*";
        // patterns for double and date
        Pattern double_pattern = Pattern.compile(doubleExpression);
        Pattern driver_pattern = Pattern.compile(driverIdExpression);

        private Text curDriver = new Text();

        public void map(Object key, Text value, Context context
        ) {

            DoubleWritable revenue = new DoubleWritable();
            try {
                String inputLine = value.toString();
                String[] lineData = inputLine.split(",");
                Matcher driverMatcher = driver_pattern.matcher(lineData[1]);
                Matcher dbMatcher = double_pattern.matcher(lineData[10]);
                if (driverMatcher.matches() && dbMatcher.matches()) {
                    curDriver.set(lineData[1]);
                    revenue.set(Double.parseDouble(lineData[10]));
                    context.write(curDriver, revenue);
                }

            } catch (Exception e) {
//                throw new IOException(e);
            }
        }
    }


        public static class RevenueReducer
                extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


            public void reduce(Text key, Iterable<DoubleWritable> values,
                               Context context
            ) throws IOException, InterruptedException {
                double sum = 0;
                for (DoubleWritable val : values) {
                    sum += val.get();
                }
                DoubleWritable result = new DoubleWritable(sum);
                context.write(key, result);
            }

        }

        public int run(String[] args) throws Exception {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Revenue count");
            job.setJarByClass(RevenueCount.class);
            job.setMapperClass(RevenueMapper.class);
            job.setCombinerClass(RevenueReducer.class);
            job.setReducerClass(RevenueReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            List<String> other_args = new ArrayList<String>();
            for (int i = 0; i < args.length; ++i) {
                try {
                    if ("-r".equals(args[i])) {
                        job.setNumReduceTasks(Integer.parseInt(args[++i]));
                    } else {
                        other_args.add(args[i]);
                    }
                } catch (NumberFormatException except) {
                    System.out.println("ERROR: Integer expected instead of " + args[i]);
                    return printUsage();
                } catch (ArrayIndexOutOfBoundsException except) {
                    System.out.println("ERROR: Required parameter missing from " +
                            args[i - 1]);
                    return printUsage();
                }
            }
            // Make sure there are exactly 2 parameters left.
            if (other_args.size() != 2) {
                System.out.println("ERROR: Wrong number of parameters: " +
                        other_args.size() + " instead of 2.");
                return printUsage();
            }
            FileInputFormat.setInputPaths(job, other_args.get(0));
            FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
            return (job.waitForCompletion(true) ? 0 : 1);
        }

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new RevenueCount(), args);
            System.exit(res);
        }
}