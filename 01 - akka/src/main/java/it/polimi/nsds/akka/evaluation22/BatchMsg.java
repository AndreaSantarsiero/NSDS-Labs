package it.polimi.nsds.akka.evaluation22;


public class BatchMsg {
	
	private boolean isOn;
	
	public BatchMsg(boolean isOn) {
		this.isOn = isOn;
	}

	public boolean isOn() {
		return isOn;
	}
}
