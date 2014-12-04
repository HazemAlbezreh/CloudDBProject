package app_kvServer;

public enum ServerStatus {
	STARTED,					/* Server process requests*/
	STOPED,						/* Server waiting */
	LOCKED,						/* Locked for data transfer */
	SHUTDOWNED					/* Quit */
}
