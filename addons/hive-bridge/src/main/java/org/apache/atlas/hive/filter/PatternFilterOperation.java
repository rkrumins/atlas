package org.apache.atlas.hive.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternFilterOperation implements FilterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFilterOperation.class);

    List<String> regexPatternList;

    public PatternFilterOperation(List<String> list) {
        this.regexPatternList = list;
    }

    public static boolean getValidMatchForRegexPattern(String databaseName, String regexPattern) {
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(databaseName.toLowerCase());

        return matcher.matches();
    }

    public static boolean checkIfDatabaseIsOfValidRegexPattern(String databaseName, List<String> regexPatterns) {
        boolean validPatternFlag = false;

        for (String regexPattern: regexPatterns) {
            LOG.debug("Checking if {} follows regex pattern {}", databaseName, regexPattern);
            if (getValidMatchForRegexPattern(databaseName, regexPattern)) {
                LOG.debug("Database {} matches the regex pattern {}", databaseName, regexPattern);
                validPatternFlag = true;
                break;
            }
        }
        return validPatternFlag;
    }

    @Override
    public boolean evaluate(String databaseName) {
        boolean validFlag = checkIfDatabaseIsOfValidRegexPattern(databaseName, regexPatternList);
        LOG.debug("Flag for database {} in valid databases set equals to {}", databaseName, validFlag);
        return validFlag;
    }
}
