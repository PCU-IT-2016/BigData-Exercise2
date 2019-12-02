public class AirbnbRecord implements Resource {

    int id;
    String name;
    int hostId;
    String hostName;
    String neighbourGroup;
    String neighbourhood;
    float latitude;
    float longitude;
    String roomType;
    int price;
    int minimumNights;
    int numberOfReviews;
    String lastReviews;
    float reviewsPerMonth;
    int calculatedHostListingsCount;
    int availability365;

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

    public int getHostId() {
        return hostId;
    }

    public void setHostId(int hostId) {
        this.hostId = hostId;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getNeighbourGroup() {
        return neighbourGroup;
    }

    public void setNeighbourGroup(String neighbourGroup) {
        this.neighbourGroup = neighbourGroup;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public void setNeighbourhood(String neighbourhood) {
        this.neighbourhood = neighbourhood;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public String getRoomType() {
        return roomType;
    }

    public void setRoomType(String roomType) {
        this.roomType = roomType;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getMinimumNights() {
        return minimumNights;
    }

    public void setMinimumNights(int minimumNights) {
        this.minimumNights = minimumNights;
    }

    public int getNumberOfReviews() {
        return numberOfReviews;
    }

    public void setNumberOfReviews(int numberOfReviews) {
        this.numberOfReviews = numberOfReviews;
    }

    public String getLastReviews() {
        return lastReviews;
    }

    public void setLastReviews(String lastReviews) {
        this.lastReviews = lastReviews;
    }

    public float getReviewsPerMonth() {
        return reviewsPerMonth;
    }

    public void setReviewsPerMonth(float reviewsPerMonth) {
        this.reviewsPerMonth = reviewsPerMonth;
    }

    public int getCalculatedHostListingsCount() {
        return calculatedHostListingsCount;
    }

    public void setCalculatedHostListingsCount(int calculatedHostListingsCount) {
        this.calculatedHostListingsCount = calculatedHostListingsCount;
    }

    public int getAvailability365() {
        return availability365;
    }

    public void setAvailability365(int availability365) {
        this.availability365 = availability365;
    }

    @Override
    public void decode(String line) {
        String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        String[] lineValues = line.split(regex);

        this.setId(Integer.parseInt(lineValues[0]));
        this.setName(lineValues[1]);
        this.setHostId(Integer.parseInt(lineValues[2]));
        this.setHostName(lineValues[3]);
        this.setNeighbourGroup(lineValues[4]);
        this.setNeighbourhood(lineValues[5]);
        this.setLatitude(Float.parseFloat(lineValues[6]));
        this.setLongitude(Float.parseFloat(lineValues[7]));
        this.setRoomType(lineValues[8]);
        this.setPrice(Integer.parseInt(lineValues[9]));
        this.setMinimumNights(Integer.parseInt(lineValues[10]));
        this.setNumberOfReviews(Integer.parseInt(lineValues[11]));
        this.setLastReviews(lineValues[12]);
        this.setReviewsPerMonth(Float.parseFloat(lineValues[13]));
        this.setCalculatedHostListingsCount(Integer.parseInt(lineValues[14]));
        this.setAvailability365(Integer.parseInt(lineValues[15]));
    }

    @Override
    public String encode() {
        return String.valueOf(this.id);
    }
}
