package jobs;

import DataTypes.SyntacticNgram;

import java.util.Comparator;

public class SyntacticNgramComparator implements Comparator<SyntacticNgram> {
    @Override
    public int compare(SyntacticNgram o1, SyntacticNgram o2) {
        return o1.position.intValue() - o2.position.intValue();
    }
}
