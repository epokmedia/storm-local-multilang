package com.epokmedia.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author Michäel Schwartz <info@epokmedia.fr>
 */
public class StormLocalMultilang {

	/**
	 * @param args the command line arguments
	 */
	@SuppressWarnings({"unchecked"})
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		if (args.length == 0) throw new RuntimeException("Topology JSON file required");
		
		File topologyFile = new File(args[0]);
		String topologyFilePath = new File(topologyFile.getParent()).getCanonicalPath();
		
		JSONObject jsonTopology = (JSONObject)JSONValue.parse(new FileReader(topologyFile));
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		Config conf = new Config();
		
		String topologyName = (String) jsonTopology.get("name");
		JSONObject components = (JSONObject) jsonTopology.get("components");
		JSONObject spouts = (JSONObject) components.get("spouts");
		JSONObject bolts = (JSONObject) components.get("bolts");
		
		for(Entry<String, Object> spout : (Set<Entry<String, Object>>)spouts.entrySet()) {
			String componentId = spout.getKey();
			JSONObject componentDefinition = (JSONObject) spout.getValue();
			String command = (String) componentDefinition.get("command");
			String scriptPath = formatScriptPath((String) componentDefinition.get("path"), topologyFilePath);
			List<String> fields = (List<String>) componentDefinition.get("fields");
			topologyBuilder.setSpout(componentId, new ScriptSpout(command, scriptPath, fields));
		}
		
		for(Entry<String, Object> bolt : (Set<Entry<String, Object>>)bolts.entrySet()) {
			String componentId = bolt.getKey();
			JSONObject componentDefinition = (JSONObject) bolt.getValue();
			
			String command = (String) componentDefinition.get("command");
			String scriptPath = formatScriptPath((String) componentDefinition.get("path"), topologyFilePath);
			List<String> fields = (List<String>) componentDefinition.get("fields");
			
			BoltDeclarer boltDeclarer = topologyBuilder.setBolt(componentId, new ScriptBolt(command, scriptPath, fields));
			
			JSONArray groupings = (JSONArray) componentDefinition.get("grouping");
			for (Object grouping : groupings) {
				String groupingComponentId = (String) ((JSONObject) grouping).get("componentId");
				String groupingType = (String) ((JSONObject) grouping).get("type");
				
				if (groupingType.equals("shuffle")) {
					boltDeclarer.shuffleGrouping(groupingComponentId);
				}
				
				if (groupingType.equals("fields")) {
					List<String> groupingFields = (List<String>) ((JSONObject) grouping).get("fields");
					boltDeclarer.fieldsGrouping(groupingComponentId, new Fields(groupingFields));
				}
				
			}
			
		}
		
		conf.setDebug(true);
		conf.putAll((Map<String, Object>) jsonTopology.get("configuration"));
		
		StormTopology topology = topologyBuilder.createTopology();
	
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);
		
	}

	private static String formatScriptPath(String scriptPath, String topologyFilePath) {
		
		File scriptFile;
		if (scriptPath.startsWith("/")) {
			scriptFile = new File(scriptPath);
		} else {
			scriptFile = new File(topologyFilePath.replaceAll("/$", "") + "/" + scriptPath);
		}
		
		if (scriptFile.exists()) return scriptFile.getAbsolutePath();
		else return scriptPath;
		
	}

	
	private static class ScriptBolt extends ShellBolt implements IRichBolt {
		private static final long serialVersionUID = 1L;

		protected List<String> fields;
		
		public ScriptBolt(String command, String codeResource, List<String> fields) {
			super(command, codeResource);
			
			this.fields = fields;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields(fields));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return Collections.emptyMap();
		}
		 
    }
	
	private static class ScriptSpout extends ShellSpout implements IRichSpout {
		private static final long serialVersionUID = 1L;

		protected List<String> fields;
		
		public ScriptSpout(String command, String codeResource, List<String> fields) {
			super(command, codeResource);
			
			this.fields = fields;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields(fields));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return Collections.emptyMap();
		}
		 
    }
}
