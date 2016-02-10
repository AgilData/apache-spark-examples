import java.io.Serializable;

class JPopulation implements Serializable {

    private String logrecno;
    private int male;
    private int female;

    public JPopulation(String logrecno, int male, int female) {
        this.logrecno = logrecno;
        this.male = male;
        this.female = female;
    }

    public String getLogrecno() {
        return logrecno;
    }

    public void setLogrecno(String logrecno) {
        this.logrecno = logrecno;
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

    @Override
    public String toString() {
        return "Population{" +
                "logrecno='" + logrecno + '\'' +
                ", male=" + male +
                ", female=" + female +
                '}';
    }
}
