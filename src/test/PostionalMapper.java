package test;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PostionalMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {




	    String fullLine[] = value.toString().split(" ");
	    String DocId = ((FileSplit) context.getInputSplit()).getPath().getName();
	    int dotIndex = DocId.lastIndexOf('.');
	    if(dotIndex != -1 && dotIndex !=0){
	    	DocId = DocId.substring(0, dotIndex);
	    }
	    
		for(int i = 0 ; i< fullLine.length ; i++) {  
			context.write(new Text(fullLine[i]), new Text(DocId + ":" + i));
		}

		

	}

}