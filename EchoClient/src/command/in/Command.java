package command.in;

public interface Command {
	/**
	 * Execute the command
	 * @return The command response
	 */
	public String execute();
	
	/**
	 * Gets the usage information of a command
	 * @return the usage data
	 */
	public String getHelpText();

}
