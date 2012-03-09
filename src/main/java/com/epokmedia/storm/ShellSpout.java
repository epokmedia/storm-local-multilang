package com.epokmedia.storm;

import backtype.storm.generated.ShellComponent;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author EPOKMEDIA <info@epokmedia.fr>
 */
public class ShellSpout implements ISpout {

	public static Logger LOG = Logger.getLogger(ShellSpout.class);
	private static final long serialVersionUID = 1L;
	
	Process _subprocess;
	String subpid;
	DataOutputStream _processin;
	BufferedReader _processout;
	SpoutOutputCollector _collector;
	String[] command;
	
	public ShellSpout(ShellComponent component) {
		this(component.get_execution_command(), component.get_script());
	}

	public ShellSpout(String... command) {
		this.command = command;
	}

	private String initializeSubprocess(TopologyContext context) {

		//can change this to launchSubprocess and have it return the pid (that the subprcess returns)
		ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(context.getCodeDir()));
		try {
			_subprocess = builder.start();
			_processin = new DataOutputStream(_subprocess.getOutputStream());
			_processout = new BufferedReader(new InputStreamReader(
					_subprocess.getInputStream()));

			sendToSubprocess(context.getPIDDir());

			//subprocesses must send their pid first thing
			subpid = _processout.readLine();

			LOG.info("Launched subprocess with pid " + subpid);

			return subpid;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void open(Map stormConf, TopologyContext context,
					 SpoutOutputCollector collector) {
		initializeSubprocess(context);
		_collector = collector;

		sendToSubprocess(JSONValue.toJSONString(stormConf));
		sendToSubprocess(context.toJSONString());
	}

	@Override
	public void nextTuple() {
		this.sendToSubprocess("next");

		try {

			while (true) {

				String line = "";

				while (true) {

					String subline = _processout.readLine();
					if (subline == null) {
						throw new RuntimeException(
								"Pipe to subprocess seems to be broken!");
					}
					if (subline.equals("sync")) {
						line = subline;
						break;
					}
					if (subline.equals("end")) {
						break;
					}
					if (line.length() != 0) {
						line += "\n";
					}
					line += subline;
				}

				if (line.equals("sync")) {

					break;

				} else {
					
					Map action = (Map) JSONValue.parse(line);
					String cmd = (String) action.get("command");

					if (cmd.equals("emit")) {
						Object id = action.get("id");
						String stream = (String) action.get("stream");
						if (stream == null) {
							stream = Utils.DEFAULT_STREAM_ID;
						}
						Long task = (Long) action.get("task");
						@SuppressWarnings("unchecked")
						List<Object> tuple = (List) action.get("tuple");
						
						if (task == null) {
							List<Integer> outtasks = _collector.emit(stream, tuple, id);
							sendToSubprocess(JSONValue.toJSONString(outtasks));
						} else {
							_collector.emitDirect((int) task.longValue(), stream, tuple, id);
						}

					} else if (cmd.equals("log")) {
						String msg = (String) action.get("msg");
						LOG.info("Shell msg: " + msg);
					}

				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Error during multilang processing", e);
		}

	}

	@Override
	@SuppressWarnings("unchecked")
	public void ack(Object o) {
		JSONObject obj = new JSONObject();
		obj.put("command", "ack");
		obj.put("id", o);

		this.sendToSubprocess(obj.toString());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void fail(Object o) {
		JSONObject obj = new JSONObject();
		obj.put("command", "fail");
		obj.put("id", o);

		this.sendToSubprocess(obj.toString());
	}

	@Override
	public void close() {
		_subprocess.destroy();
		_processin = null;
		_processout = null;
		_collector = null;
	}

	private void sendToSubprocess(String str) {
		try {
			_processin.writeBytes(str + "\n");
			_processin.writeBytes("end\n");
			_processin.flush();
		} catch (IOException ex) {
			LOG.warn("failed to write to subprocess : " + subpid, ex);
		}
	}
}