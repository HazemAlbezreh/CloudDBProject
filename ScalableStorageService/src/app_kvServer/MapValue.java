package app_kvServer;

public class MapValue {

		private String value;
		private int counter;
		
		/**
		 * @return the counter
		 */

		public MapValue(){
			
		}
		
		public MapValue(String value,int counter){
			this.value = value;
			this.counter = counter;
		}
		
		public int getCounter() {
			return counter;
		}
		
		/**
		 * @param counter the counter to set
		 */
		public void setCounter(int counter) {
			this.counter = counter;
		}
		
		/**
		 * @return the value
		 */
		public String getValue() {
			return value;
		}
		
		/**
		 * @param value the value to set
		 */
		public void setValue(String value) {
			this.value = value;
		}

}
