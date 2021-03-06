//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2013.12.11 at 01:20:03 PM CET 
//


package config;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.log4j.Logger;




/**
 * <p>Java class for ServerInfo complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="ServerInfo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="serverIP" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="port" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ServerInfo", propOrder = {
		"serverIP",
		"port"
})
public class ServerInfo implements  java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@XmlElement(required = true)
	protected String serverIP;
	protected int port;

	/**
	 * Gets the value of the serverIP property.
	 * 
	 * @return
	 *     possible object is
	 *     {@link String }
	 *     
	 */
	public ServerInfo(){}

	public ServerInfo(String ip,int port){
		this.serverIP=ip;
		this.port=port;
	}

	public String getServerIP() {
		return serverIP;
	}

	private static Logger logger = Logger.getLogger(ServerInfo.class);

	/**
	 * Sets the value of the serverIP property.
	 * 
	 * @param value
	 *     allowed object is
	 *     {@link String }
	 *     
	 */
	public void setServerIP(String value) {
		this.serverIP = value;
	}

	/**
	 * Gets the value of the port property.
	 * 
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the value of the port property.
	 * 
	 */
	public void setPort(int value) {
		this.port = value;
	}

	@Override
	public String toString() {
		return this.getServerIP()+":"+this.getPort(); 
	}

	public boolean runServerRemotly(String path) {
		//TODO SSh 
//		String script = "ssh -n " + this.getServerIP() + " nohup java -jar " + path + 
//		"/ms3-server.jar " + this.getPort();
		String script= "java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address="+(this.getPort()+100)+" -jar ms3-server.jar "+this.getPort();

		//String script = "java -jar "+ path + "/ms3-server.jar " + this.getPort();
		Runtime runtime = Runtime.getRuntime();


		try {
			Process p = runtime.exec(script);
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s = br.readLine();
			while (s == null || !s.contains("Initialize server ...")) {
				s = br.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Error cannot run server: " + this.serverIP+"remotly");
			return false;
		}
		return true;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.getServerIP() == null) ? 0 : getServerIP().hashCode());
		result = prime * result + this.getPort();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ServerInfo))
			return false;
		ServerInfo other = (ServerInfo) obj;
		if (this.getServerIP() == null) {
			if (other.getServerIP() != null)
				return false;
		} else if (!this.getServerIP().equals(other.getServerIP()))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

}
