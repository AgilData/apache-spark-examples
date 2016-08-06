import java.io.Serializable;

public class JGeo implements Serializable {

    private String logrecno;
    private String name;
    private String sumlev;

    public JGeo() {
    }

    public JGeo(String logrecno, String name, String sumlev) {
        this.logrecno = logrecno;
        this.name = name;
        this.sumlev = sumlev;
    }

    public String getLogrecno() {
        return logrecno;
    }

    public void setLogrecno(String logrecno) {
        this.logrecno = logrecno;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSumlev() {
        return sumlev;
    }

    public void setSumlev(String sumlev) {
        this.sumlev = sumlev;
    }

    @Override
    public String toString() {
        return "JGeo{" +
                "logrecno='" + logrecno + '\'' +
                ", name='" + name + '\'' +
                ", sumlev='" + sumlev + '\'' +
                '}';
    }
}
