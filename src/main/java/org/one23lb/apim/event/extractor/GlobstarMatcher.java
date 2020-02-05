package org.one23lb.apim.event.extractor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * https://en.wikipedia.org/wiki/Glob_(programming)
 */
public class GlobstarMatcher
{
    private static final Pattern WILDCARDS = Pattern.compile("(\\*\\*/)|(\\*)");

    private final Pattern itsGlobRegex;
    private final String itsBaseDir;

    public GlobstarMatcher(final String globPattern)
    {
        if (globPattern.indexOf("***") >= 0)
            throw new IllegalArgumentException("GLOB cannot have more than 2 consecutive asterisks: " + globPattern);

        String regexPattern = globPattern.replace('\\', '/').replaceAll("//+", "/");

        if (regexPattern.endsWith("/"))
        {
            // This means all files in current directory
            // but do not recurse.
            // If the user wants to recurse, it must end with /**
            regexPattern = regexPattern + "*";
        }
        else if (regexPattern.endsWith("/**"))
        {
            // This means all files in current directory
            // or any of its subdirectories.
        	regexPattern = regexPattern + "/*";
        }

        // TODO As mentioned in https://en.wikipedia.org/wiki/Glob_(programming)
        //	[!abc] should be converted to [^abc]

        final int firstWildcard = indexOfAny(regexPattern, '*', '?', '[');

        if (firstWildcard < 0)
        {
            itsBaseDir = regexPattern;
        }
        else
        {
        	final int lastSlash = regexPattern.lastIndexOf('/', firstWildcard);

            if (lastSlash < 0)
            {
            	itsBaseDir = regexPattern.startsWith("/") ? "/" : "";
            }
            else
            {
                itsBaseDir = regexPattern.substring(0, lastSlash);
            }

            regexPattern = regexPattern
                    .replaceAll("[.]", "[.]")
                    .replaceAll("[?]", "[^/]")
                ;

            final Matcher m = WILDCARDS.matcher(regexPattern);
            final StringBuffer sb = new StringBuffer(regexPattern.length() * 5 / 4);

            while (m.find())
            {
            	final String replacement;

            	if (m.group(1) != null)
            		replacement = "(?:.*/)?";
            	else if (m.group(2) != null)
            		replacement = "[^/]*";
            	else
            		throw new IllegalStateException("Bug in our code.");

            	m.appendReplacement(sb, replacement);
            }

            m.appendTail(sb);

            regexPattern = sb.toString();
        }

        itsGlobRegex = Pattern.compile("^" + regexPattern + "$");
    }

    private int indexOfAny(final String str, final char... cs)
    {
    	int index = Integer.MAX_VALUE;

    	for (final char c : cs)
    	{
    		final int offset = str.indexOf(c);

    		if (offset >= 0 && offset < index)
    			index = offset;
    	}

    	return index == Integer.MAX_VALUE ? -1 : index;
    }

    public String getBaseDir()
    {
    	return itsBaseDir;
    }

    public boolean matchesFullPath(final String fullFilename)
    {
        return itsGlobRegex.matcher(fullFilename).matches();
    }
}
