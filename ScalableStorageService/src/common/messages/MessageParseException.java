package common.messages;

public class MessageParseException extends Exception{

	public MessageParseException(String s){
		super("Error Parsing Message : " + s);
	}
	
	public MessageParseException(){
		super("Error Parsing Message");
	}
}
