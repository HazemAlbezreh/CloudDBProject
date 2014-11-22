package app_kvEcs;
import config.*;

public class ConfigReader {
	private Config config;
	
	public ConfigReader(String filePath){
		//this.setConfig(XMLWrapper.readFromXML());
		FileReader reader=new FileReader(filePath);
		this.setConfig(reader.readConfiguration());
	}

	/**
	 * @return the config
	 */
	public Config getConfig() {
		return config;
	}

	/**
	 * @param config the config to set
	 */
	public void setConfig(Config config) {
		this.config = config;
	}
	
}
