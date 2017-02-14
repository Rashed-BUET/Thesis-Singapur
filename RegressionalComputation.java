import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;

public class RegressionalComputation extends Configured implements Tool {
    public static ArrayList jobs;
	
	public static class SumXTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	    	//System.out.println(value.toString());
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				y = Double.parseDouble(itr.nextToken());
	            System.out.println("sumx" +  x);
				//System.out.println("y " +  y);
 	        	context.write(new Text("sum"),new DoubleWritable(x));
            } 
        }
    }

    public static class SumXReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
	    	Double n = 0.0;
            for(DoubleWritable value:values)
                {
                    Double temp=value.get();
				    n++;
				    sum +=temp;
		    		System.out.println("reduce"+sum);
                }
            	context.write(key,new DoubleWritable(sum));
	    		context.write(new Text("n"),new DoubleWritable(n));
        }
    }

    public static class SumXSquareTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				x = x*x;
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("sumx^2"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
            
        }
    }



  	public static class SumXSquareReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
            for(DoubleWritable value:values)
            {
                Double temp=value.get();
			    sum +=temp;
			    System.out.println("reduce"+sum);
            }
            context.write(key,new DoubleWritable(sum));
        }
    }

    public static class SumXCubeTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				x = x*x*x;
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
        }
    }

  	public static class SumXCubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
            for(DoubleWritable value:values)
            {
                Double temp=value.get();
			    sum +=temp;
			    System.out.println("reduce"+sum);
            }
            context.write(key,new DoubleWritable(sum));
        }
    }

    public static class SumXToFourTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	 
            StringTokenizer itr=new StringTokenizer(value.toString());
	    	//n = a = itr.nextToken();
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				x = x*x*x*x;
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
        }
    }



  	public static class SumXToFourReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
	    public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
	    {
	        Double sum=0.0;
	        for(DoubleWritable value:values)
            {
                Double temp=value.get();
			    sum +=temp;
			    System.out.println("reduce"+sum);
            }
	        context.write(key,new DoubleWritable(sum));
	    	//context.write(new Text("n"),new DoubleWritable(5.0));
	    }
	}

	public static class SumYTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
	    	//n = a = itr.nextToken();
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(y));
            }
        }
    }



  	public static class SumYReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
            for(DoubleWritable value:values)
            {
                Double temp=value.get();
			    sum +=temp;
			    System.out.println("reduce"+sum);
            }
            context.write(key,new DoubleWritable(sum));
	    	//context.write(new Text("n"),new DoubleWritable(5.0));
        }
    }

    public static class SumXYTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	 
            StringTokenizer itr=new StringTokenizer(value.toString());
	    	//n = a = itr.nextToken();
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x*y));
            }
        }
    }



  	public static class SumXYReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
            for(DoubleWritable value:values)
            {
                Double temp=value.get();
			    sum +=temp;
			    System.out.println("reduce"+sum);
            }
            context.write(key,new DoubleWritable(sum));
	    	//context.write(new Text("n"),new DoubleWritable(5.0));
        }
    }

    public static class SumXSquareYTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	 
            StringTokenizer itr=new StringTokenizer(value.toString());
	    	//n = a = itr.nextToken();
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				y = Double.parseDouble(itr.nextToken());
	        	System.out.println("map"+x);
 	        	context.write(new Text("sum"),new DoubleWritable(x*x*y));
            }  
        }
    }



  	public static class SumXSquareYReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
            for(DoubleWritable value:values)
            {
                Double temp=value.get();
			    sum +=temp;
			    System.out.println("reduce"+sum);
            }
            context.write(key,new DoubleWritable(sum));
	    //context.write(new Text("n"),new DoubleWritable(5.0));
        }
    }


    public static void main(String[] args) throws Exception 
    {
    	String myArgs[];
        myArgs = new String[1];
        jobs = new ArrayList();
        int size = 0;
    	try 
    	{
            InputStream fis = new FileInputStream("sample.txt");
            InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
            BufferedReader br = new BufferedReader(isr);
            String line;
            line = br.readLine();
            int jobSize = Integer.parseInt(line);
            int a = jobSize;
            myArgs = new String[jobSize+4];
            myArgs[0] = line;
            myArgs[1] = "0";
            int i = 2;
            while (jobSize != 0) 
            {
                line = br.readLine();
                myArgs[i++] = line;
                System.out.println(line);
                jobSize--;
            }
            size = Integer.parseInt(myArgs[0]);
	    myArgs[a+2] = args[args.length-2];
            myArgs[a+3] = args[args.length-1];
        } 
        catch (Exception e) 
        {
            System.out.println(Arrays.toString(e.getStackTrace()));
        } 

        

        for (int i = 0; i < size; i++) 
        {
        	int res = ToolRunner.run(new Configuration(), new RegressionalComputation(), myArgs);
            //t.print(myArgs);
            int iter = Integer.parseInt(myArgs[1]);
            iter++;
            myArgs[1] = String.valueOf(iter);
            //System.out.println("");
        }
        for (int i = 0; i < jobs.size(); i++) {
            Job job = (Job)jobs.get(i);
            if (!job.isComplete()) {
                i--;
            }
        }
    }

	@Override

	public int run(String args[]) throws Exception
	{
        
		int iter = Integer.parseInt(args[1]);
        System.out.println("In Run ..................... "+iter);
        if (iter == 0) {
		System.out.println(args[args.length-2]);
        	Configuration conf = this.getConf();
		
			Job job=Job.getInstance(conf, "sumX");
			job.setJarByClass(RegressionalComputation.class);    
			
			job.setMapperClass(SumXTokenizerMapper.class);
			job.setReducerClass(SumXReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			 
			FileInputFormat.addInputPath(job, new Path(args[args.length-2]));
			FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]+"/"+args[iter+2]));

			job.submit();
            jobs.add(job);
        }

        else if (iter == 1) {
            Configuration conf1 = this.getConf();
        	Job job1=Job.getInstance(conf1, "sumXSquare");
            job1.setJarByClass(RegressionalComputation.class);    
            
            job1.setMapperClass(SumXSquareTokenizerMapper.class);
            job1.setReducerClass(SumXSquareReducer.class);
            
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);
             
            FileInputFormat.addInputPath(job1, new Path(args[args.length-2]));
            FileOutputFormat.setOutputPath(job1, new Path(args[args.length-1]+"/"+args[iter+2]));
            job1.submit();
            jobs.add(job1);


        }

        else if (iter == 2) {
            Configuration conf2 = this.getConf();
        	Job job2=Job.getInstance(conf2, "sumXCube");
            job2.setJarByClass(RegressionalComputation.class);    
            
            job2.setMapperClass(SumXCubeTokenizerMapper.class);
            job2.setReducerClass(SumXCubeReducer.class);
            
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
             
            FileInputFormat.addInputPath(job2, new Path(args[args.length-2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[args.length-1]+"/"+args[iter+2])); 
            job2.submit();
            jobs.add(job2);
    	
        }

        else if (iter == 3) {
        	Configuration conf3 = this.getConf();
        	Job job3=Job.getInstance(conf3, "xToFour");
            job3.setJarByClass(RegressionalComputation.class);    
            
            job3.setMapperClass(SumXToFourTokenizerMapper.class);
            job3.setReducerClass(SumXToFourReducer.class);
            
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(DoubleWritable.class);
             
            FileInputFormat.addInputPath(job3, new Path(args[args.length-2]));
            FileOutputFormat.setOutputPath(job3, new Path(args[args.length-1]+"/"+args[iter+2]));
            job3.submit();
            jobs.add(job3);

        }

        else if (iter == 4) {
            Configuration conf4 = this.getConf();
        	Job job4=Job.getInstance(conf4, "sumY");
            job4.setJarByClass(RegressionalComputation.class);    
            
            job4.setMapperClass(SumYTokenizerMapper.class);
            job4.setReducerClass(SumYReducer.class);
            
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(DoubleWritable.class);
             
            FileInputFormat.addInputPath(job4, new Path(args[args.length-2]));
            FileOutputFormat.setOutputPath(job4, new Path(args[args.length-1]+"/"+args[iter+2]));
            job4.submit();
            jobs.add(job4);
       	
        }

        else if (iter == 5) {
            Configuration conf5 = this.getConf();
        	Job job5=Job.getInstance(conf5, "sumXY");
            job5.setJarByClass(RegressionalComputation.class);    
            
            job5.setMapperClass(SumXYTokenizerMapper.class);
            job5.setReducerClass(SumXYReducer.class);
            
            job5.setOutputKeyClass(Text.class);
            job5.setOutputValueClass(DoubleWritable.class);
             
            FileInputFormat.addInputPath(job5, new Path(args[args.length-2]));
            FileOutputFormat.setOutputPath(job5, new Path(args[args.length-1]+"/"+args[iter+2]));
            job5.submit();
            jobs.add(job5);
           	
        }

        else if (iter == 6) {
             Configuration conf6 = this.getConf();
        	Job job6=Job.getInstance(conf6, "sumXSquareY");
            job6.setJarByClass(RegressionalComputation.class);    
            
            job6.setMapperClass(SumXSquareYTokenizerMapper.class);
            job6.setReducerClass(SumXSquareYReducer.class);
            
            job6.setOutputKeyClass(Text.class);
            job6.setOutputValueClass(DoubleWritable.class);
             
            FileInputFormat.addInputPath(job6, new Path(args[args.length-2]));
            FileOutputFormat.setOutputPath(job6, new Path(args[args.length-1]+"/"+args[iter+2]));
            job6.submit();
            jobs.add(job6);
         	
        }
        return 1;
        
        
	}




}
