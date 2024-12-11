package test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import java.util.*;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PositionalReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		HashMap<String,HashSet<Integer>> docPosition = new HashMap<>();
		
		for (Text value : values) {
			String doc_pos[] = value.toString().split(":");
			String docID = doc_pos[0];
			int position = Integer.parseInt(doc_pos[1]);
			
			if (!docPosition.containsKey(docID)) {
				docPosition.put(docID, new HashSet<Integer>());
			}
			docPosition.get(docID).add(position);
		}
		
		StringBuilder postingList = new StringBuilder();
		for (Map.Entry<String, HashSet<Integer>> entry : docPosition.entrySet()) {
            String docId = entry.getKey();
            ArrayList<Integer> list = new ArrayList<Integer>(entry.getValue());
            Collections.sort(list);
            StringBuilder positions = new StringBuilder() ;
            for(int i=0;i<list.size();i++){
            	positions.append(list.get(i).toString());
            	if(i< list.size()-1){
            		positions.append(",");
            	}
            }
            
            postingList.append(docId).append(":").append(positions).append(";");
        }

        context.write(key, new Text(postingList.toString().trim()));
	}

}