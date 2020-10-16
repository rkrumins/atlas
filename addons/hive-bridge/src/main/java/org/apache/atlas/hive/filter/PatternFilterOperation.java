package org.apache.atlas.hive.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternFilterOperation implements FilterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFilterOperation.class);

    List<String> regexPatternList;
    Set<String> regexPatternSet;

    public PatternFilterOperation(List<String> patterList) {
        this.regexPatternList = patterList;
    }

    public PatternFilterOperation(Set<String> patternSet) {
        this.regexPatternSet = patternSet;
    }

    public boolean getValidMatchForRegexPattern(String databaseName, String regexPattern) {
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher = pattern.matcher(databaseName.toLowerCase());

        return matcher.matches();
    }

    public boolean checkIfDatabaseIsOfValidRegexPattern(String databaseName, List<String> regexPatterns) {
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

    public boolean checkIfDatabaseIsOfValidRegexPattern(String databaseName, Set<String> regexPatterns) {
        boolean validPatternFlag = false;

        for (String regexPattern : regexPatterns) {
            if (getValidMatchForRegexPattern(databaseName, regexPattern)) {
                LOG.debug("Database {} matches the regex pattern {}", databaseName, regexPattern);
                validPatternFlag = true;
                break;
            }
        }
        return validPatternFlag;
    }

    public boolean checkIfValidDatabase(String databaseName){
        if (this.regexPatternSet != null) {
            return checkIfDatabaseIsOfValidRegexPattern(databaseName, regexPatternSet);
        } else if (this.regexPatternList != null) {
            return checkIfDatabaseIsOfValidRegexPattern(databaseName, regexPatternList);
        }
        LOG.error("Could not evaluate if valid database");
        return false;
    }

    @Override
    public boolean evaluate(String databaseName) {
        boolean validFlag = checkIfValidDatabase(databaseName);
        LOG.debug("Flag for database {} in valid databases set equals to {}", databaseName, validFlag);
        return validFlag;
    }
}
