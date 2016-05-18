package ngram.lib;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

public class NgramParser {
    private int adjWordCount;
    private ArrayList<Character> initials = new ArrayList<>();
    private boolean initialized = false;

    public NgramParser(int adjWordCount) {
        this.adjWordCount = adjWordCount;
    }

    public void addLine(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            char firstChar = tokenizer.nextToken().charAt(0);
            initials.add(firstChar);
        }

        if (!initialized && hasNext()) {
            int lastIndex = -1;
            for (int x=0; x < adjWordCount; x++) {
                if (!Character.isLetter(initials.get(x))) {
                    lastIndex = x;
                }
            }
            for (int x=0; x <= lastIndex; x++) {
                shift();
            }
            if (hasNext()) initialized = true;
        }
    }

    public boolean hasNext() {
        return initials.size() >= adjWordCount;
    }

    public List<Character> next() {
        if (!hasNext()) {
            return null;
        }
        /* 
         * if the end of this Ngram is not character, drop the suroundings,
         * inclusive
         */
        int x = 0;
        while (true) {
            if (!(Character.isLetter(initials.get(adjWordCount-1)))) {
                x = adjWordCount;
            }
            if (x <= 0) break;
            shift();
            x--;
            if (!hasNext()) {
                for (; x > 0; x--) shift();
                initialized = false;
                return null;
            }
        }
        return initials.subList(0, adjWordCount);
    }

    public void shift() {
        initials.remove(0);
    }
}

