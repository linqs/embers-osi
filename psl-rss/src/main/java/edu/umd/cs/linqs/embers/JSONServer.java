package edu.umd.cs.linqs.embers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.configuration.ConfigurationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.ConfigManager;

/**
 * 
 * Simple server listens for JSON objects on given port and runs JSONProcessor 
 * on received JSON objects, and returns result on the same port
 * 
 * @author Bert Huang <bert@cs.umd.edu>
 *
 */
public class JSONServer {
	
	/**
	 * Prefix of property keys used by this class.
	 * 
	 * @see ConfigManager
	 */
	public static final String CONFIG_PREFIX = "jsonserver";
	
	
	/* Config key for port */
	public final String PORT_KEY = CONFIG_PREFIX + ".port";
	public final int PORT_DEFAULT = 9999;
	
	/* Config key for JSON processor type */
	public final String PROCESSOR_KEY = CONFIG_PREFIX + ".processor";
	public final String PROCESSOR_DEFAULT = "DummyProcessor";
	
	private ServerSocket server;
	private Socket socket;
	private final int port;	
	private final JSONProcessor processor;
	private Logger log = LoggerFactory.getLogger(this.getClass());
	
	public JSONServer(int port, JSONProcessor processor) {
		this.port = port;
		this.processor = processor;
	}
	
	public JSONServer(ConfigBundle config) throws ConfigurationException {		
		port = config.getInt(PORT_KEY, PORT_DEFAULT);
		String processorType = config.getString(PROCESSOR_KEY, PROCESSOR_DEFAULT);
		
		processor = getProcessor(processorType);
	}

	private JSONProcessor getProcessor(String processorType) {
		if (processorType.equals("RSSLocationProcessor"))
			return new RSSLocationProcessor();
		else
			return new DummyProcessor();
	}

	public void start() {
		log.debug("Starting server");
		try {
			server = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}

		int count = 0;
		
		log.debug("Server started");
		while (true) {
			try {
				socket = server.accept();

				log.debug("Client connected");
				
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "utf-8"));
				OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(), "utf-8");
				while (socket.isConnected()) {
					try {
						String line = in.readLine();
						log.trace(line);
						JSONObject input = new JSONObject(line);
						JSONObject json = processor.process(input);
						log.debug("Processed JSON object. Writing out");
						json.write(out);
						out.write('\n');
						out.flush();
						count++;
						log.debug("Processed {} messages", count);
					} catch (JSONException e) {
						log.warn("Unable to read JSON object from line");
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String [] args) throws ConfigurationException {
//		JSONServer server = new JSONServer(9999, new DummyProcessor());
		ConfigManager cm = ConfigManager.getManager();
		ConfigBundle cb = cm.getBundle("rss");
		JSONServer server = new JSONServer(cb);
		
		server.start();
	}

}
