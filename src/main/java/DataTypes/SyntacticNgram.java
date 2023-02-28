package DataTypes;

public class SyntacticNgram {

    public Long position ;
    public String type ;
    public String head_word;

    public  String typeInSentence ;

    public Long numOfOccurrences;

    public SyntacticNgram(String head_word,   String type, String typeInSentence,Long position, Long numOfOccurrences ){

        this.head_word = head_word;
        this.type = type;
        this.typeInSentence = typeInSentence;
        this.position = position;
        this.numOfOccurrences = numOfOccurrences;

    }
    @Override
    public String toString() {
        return head_word+ '\t'+type + '\t' + typeInSentence +  '\t' +position.toString();
    }
}


