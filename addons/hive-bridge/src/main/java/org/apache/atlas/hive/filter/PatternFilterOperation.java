package org.apache.atlas.hive.filter;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternFilterOperation implements FilterStrategy {

    List<String> regexPatternList;

    public PatternFilterOperation(List<String> list) {
        this.regexPatternList = list;
    }

    public static boolean getValidMatchForRegexPattern(String databaseName, String regexPattern) {
        Pattern pattern = Pattern.compile(regexPattern, Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(databaseName);
        return matcher.matches();
    }

    public static boolean checkIfDatabaseIsOfValidRegexPattern(String databaseName, List<String> regexPatterns) {
        boolean validPatternFlag = false;

        for (String regexPattern: regexPatterns) {
            if (getValidMatchForRegexPattern(databaseName, regexPattern)) {
                validPatternFlag = true;
                break;
            }
        }
        return validPatternFlag;
    }

    @Override
    public boolean evaluate(String databaseName) {
        return checkIfDatabaseIsOfValidRegexPattern(databaseName, regexPatternList);
    }
}
