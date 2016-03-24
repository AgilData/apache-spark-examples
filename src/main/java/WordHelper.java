import java.util.ArrayList;
import java.util.List;

public class WordHelper {

  public static String[] split(String line) {
    List<String> words = new ArrayList();
    StringBuilder b = new StringBuilder();
    for (int i=0; i<line.length(); i++) {
      char ch = line.charAt(i);
      if (Character.isAlphabetic(ch) || ch == '\'') {
        b.append(ch);
      } else {
        String w = b.toString().trim().toLowerCase();
        if (w.length() > 0) {
          if (w.endsWith("'s")) {
            w = w.substring(0, w.length() - 2);
          }
          words.add(w);
        }
        b.setLength(0);
      }
    }
    String ret[] = new String[words.size()];
    words.toArray(ret);
    return ret;
  }

}
