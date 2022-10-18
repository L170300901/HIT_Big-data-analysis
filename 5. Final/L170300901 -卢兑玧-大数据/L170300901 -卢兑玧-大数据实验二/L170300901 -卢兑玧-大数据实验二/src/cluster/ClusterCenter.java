public class ClusterCenter {
    private final double[] coordinates;

    public ClusterCenter(String value) {
        String[] coordinates = value.split(",");
        this.coordinates = new double[coordinates.length];
        for (int i = 0; i < coordinates.length; i++)
            this.coordinates[i] = Double.parseDouble(coordinates[i]);
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    

}
