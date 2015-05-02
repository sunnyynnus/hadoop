package hadoop;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author sunny
 *
 */
public class EquiJoin {
	
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     private Text join_key = new Text();
	     private Text emitIntermediate=new Text();

	     /**
	      *  Map function
	      */
	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       
	      //assuming input is not null and comma separated
	       String inputVal= value.toString();
	       String inputArray[]= inputVal.split(",");
	       
	       String joinkey=inputArray[1];
	       inputVal= inputVal.substring(inputVal.indexOf(',')+1);
	       String tempTuple= inputArray[0] + inputVal.substring(inputVal.indexOf(','));
	       	    	 
	       join_key.set(joinkey);
	       emitIntermediate.set(tempTuple);
	       output.collect(join_key, emitIntermediate);
	     }
	   }

	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		 
		 private Text emitOutputTuple;
		  
		   /**
		  * reduce function  
		  */
	     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
	      
	       List<Text> relationList1=new ArrayList<>();
	       List<Text> relationList2=new ArrayList<>();
	       String relName1=null;
	       
	       while (values.hasNext()) {
	    	  
	    	   Text textValue= new Text();
	    	   String tempValue=values.next().toString();
	    	   
	    	   String tempArray[]=tempValue.split(",");
	    	   textValue.set(tempValue.substring(tempValue.indexOf(',')+1));
	    	   
	    	   if(relName1!=null){
	    		 //rel1A
	    		   if(relName1.equals(tempArray[0])){
	    			   
	    			   relationList1.add(textValue);
	    		   }
	    		   else{
	    			   relationList2.add(textValue);
	    		   }
	    		   
	    	   }
	    	   else{
	    		   relName1=new String(tempArray[0]);
	    		   relationList1.add(textValue);
	    	   }
	    	   
	    	       	   
	       }
	       
	      int relation1size=relationList1.size();
	      int relation2size=relationList2.size();
	      //output should be of size m * n
	      for(int i=0; i<relation1size; i++){
	    	   for(int j=0; j<relation2size; j++){
	    		   
	               emitOutputTuple=new Text();
	               String outputString= relationList1.get(i).toString()+","+relationList2.get(j).toString();
	               
	    		   emitOutputTuple.set(outputString);
	               output.collect(key,emitOutputTuple);
	           }
	    	   
	       }
	       
	     }
	   }


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	   	
		try{
			JobConf jconf = new JobConf(NaturalJoin.class);
			jconf.setJobName("naturaljoin");
			jconf.setOutputKeyClass(Text.class);
			jconf.setOutputValueClass(Text.class);
			jconf.setMapperClass(Map.class);
			jconf.setReducerClass(Reduce.class);
			jconf.setInputFormat(TextInputFormat.class);
			jconf.setOutputFormat(TextOutputFormat.class);
			
			// delete the result folder on HDFS if it already exists.
			FileSystem fs=FileSystem.get(jconf);
	        	if (fs.exists(new Path(args[1])))
	            		fs.delete(new Path(args[1]), true);
	        
			FileInputFormat.setInputPaths(jconf, new Path(args[0]));
			FileOutputFormat.setOutputPath(jconf, new Path(args[1]));

			JobClient.runJob(jconf);
		}
		catch(Exception e){
			
		}
		
	}

}
