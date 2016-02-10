import java.io.Serializable;

class JPopulationSummary implements Serializable {

    private String name;
    private int male;
    private int female;

    public JPopulationSummary() {
    }

    public JPopulationSummary(String name, int male, int female) {
        this.name = name;
        this.male = male;
        this.female = female;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMale() {
        return male;
    }

    public void setMale(int male) {
        this.male = male;
    }

    public int getFemale() {
        return female;
    }

    public void setFemale(int female) {
        this.female = female;
    }

    public float getMFRatio() {
        return male*1.0f/female;
    }

    @Override
    public String toString() {
        return "PopulationSummary{" +
                "name='" + name + '\'' +
                ", male=" + male +
                ", female=" + female +
                '}';
    }
}
