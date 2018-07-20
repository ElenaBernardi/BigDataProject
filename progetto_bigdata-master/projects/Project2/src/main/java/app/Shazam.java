package app;

public class Shazam {
	private String shazamId;
	private String time;
	private String address;
	private String city; 
	private String user;
	private String trackId;
	private String artist;
	private String trackName;
	private String genre;
	
	//costruttori
	public Shazam() {}
	
	public Shazam(String shazamId, String time, String address, String city, String user, String trackId, String artist,
			String trackName, String genre) {
		this.shazamId = shazamId;
		this.time = time;
		this.address = address;
		this.city = city;
		this.user = user;
		this.trackId = trackId;
		this.artist = artist;
		this.trackName = trackName;
		this.genre = genre;
	}

	//getters and setters
	public String getShazamId() {
		return shazamId;
	}
	public void setShazamId(String shazamId) {
		this.shazamId = shazamId;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getTrackId() {
		return trackId;
	}
	public void setTrackId(String trackId) {
		this.trackId = trackId;
	}
	public String getArtist() {
		return artist;
	}
	public void setArtist(String artist) {
		this.artist = artist;
	}
	public String getTrackName() {
		return trackName;
	}
	public void setTrackName(String trackName) {
		this.trackName = trackName;
	}
	public String getGenre() {
		return genre;
	}
	public void setGenre(String genre) {
		this.genre = genre;
	}

	@Override
	public String toString() {
		return "Shazam [shazamId=" + shazamId + ", time=" + time + ", address=" + address + ", city=" + city + ", user="
				+ user + ", trackId=" + trackId + ", artist=" + artist + ", trackName=" + trackName + ", genre=" + genre
				+ "]";
	}
	
}
