package springcloud.rabbitmq.dto;

import java.io.Serializable;

public class Associate implements Serializable {

	private static final long serialVersionUID = -1663201722922450937L;

	int id;
	
	String name;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Associate [id=" + id + ", name=" + name + "]";
	}
}
