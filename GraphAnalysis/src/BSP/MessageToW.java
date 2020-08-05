package BSP;

public class MessageToW<I> {
	private int destID;
	private I message;
	public MessageToW(int id, I message) {
		this.destID = id;
		this.message = message;
	}
	
	public int getDestID() {
		return this.destID;
	}
	
	public I getMessage() {
		return this.message;
	}
}
