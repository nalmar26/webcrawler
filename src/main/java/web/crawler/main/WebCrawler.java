package web.crawler.main;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.jsoup.internal.StringUtil;
import web.crawler.stemmer.Stemmer;

import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class WebCrawler {
	// We'll use a fake USER_AGENT so the web server thinks the robot is a normal web browser.
    private static final String USER_AGENT =
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";
    private List<String> links = new LinkedList<String>();
    public static int maxDepthToCrawl = 10;
    public static String rootUrl = "";
    private int countOfLinks = 0;
    private Map<String,Integer> visited = new HashMap<>();
    private Set<String> brokenUrl = new HashSet<>();
    private Set<String> nonHtml = new HashSet<>();
    private Set<String> pdfFiles = new HashSet<>();
    private Set<String> graphicFiles = new HashSet<>();
    private Map<String,String> outGoingLinksMap = new HashMap<>();
    private Set<String> disAllowed = new HashSet<>();
    //private List<String> pagesToVisit = new LinkedList<String>();
    //private Set<String> words = new HashSet<String>();
    Map <Integer,Set<String>> wordDocuments = new HashMap<>();
	Set<String> listOfAllWords = new HashSet<>();

	private final java.sql.Connection conn;

	public WebCrawler() {
		try {
			conn = DriverManager.getConnection("jdbc:h2:mem:test");
			Statement stmt = conn.createStatement();

			stmt.execute("CREATE TABLE word_url_count (word VARCHAR2 , url VARCHAR2, count INT)");
			//stmt.execute("INSERT INTO word_url_count (word, url, count) VALUES ('test', 'test', 9)");
			ResultSet rs = stmt.executeQuery("SELECT * FROM word_url_count");

			while (rs.next()) {
				System.out.println(rs.getString("word") + " - " + rs.getString("url") + " - " + rs.getInt("count"));
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void printResults() {
		try {
			StringBuilder line = new StringBuilder("var wordMap = [");
			boolean firstWord = true;

			for(String word: listOfAllWords){
				if(word.length() <=2) {
					continue;
				}
				if(!firstWord){
					line.append(",");
				}
				firstWord = false;
				String query = String.format("SELECT * FROM word_url_count WHERE word = '%s' order by count desc", word);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(query);
				List<String> list = new ArrayList<>();
				//StringBuilder line = new StringBuilder("var wordMap = {" + word + ":[");
				line.append(" {'"+word+"':[");
				boolean firstRecord = true;
				while (rs.next()) {
					if(!firstRecord){
						line.append(",");
					}
					firstRecord = false;
					String w = rs.getString("word");
					String url = rs.getString("url");
					int ct = rs.getInt("count");
					System.out.println(w + " - " +  url+ " - " + ct);
					list.add(rs.getString("url"));
					line.append("{'https://").append(url).append("':").append(ct).append("}");
				}
				line.append("]}\r\n");
				//resultMap.put(word,list);

			}
			line.append("]");
			//System.out.println("Final result Map");
			System.out.println(line);
			FileWriter writer = new FileWriter("./output/myfile.js");
			writer.write("/*Sairam*/\r\n");
			writer.write(line.toString());
			writer.close();

		} catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
	/**
     * This performs all the work. It makes an HTTP request, checks the response, and then gathers
     * up all the links on the page. Perform a searchForWord after the successful crawl
     * 
     * @param url
     *            - The URL to visit
     * @return whether or not the crawl was successful
     */
    public boolean crawl(String url)
    {
    	if(!isValidLink(url)){
    		System.out.println("Access denied for crawling this URL");
    		return false;
    	}
    	
    	if(isExternalLink(url)){
    		System.out.println("External Link, this will not be crawled "+url);
    		return false;
    	}

    	
        try
        {
            Connection connection = Jsoup.connect(url).userAgent(USER_AGENT);
            //connection.ignoreContentType(true);
            if(url.trim().endsWith("pdf")){
            	pdfFiles.add(url);
            	return false;
        	}else if(url.trim().endsWith("jpg")|| url.trim().endsWith("jpg")){
            	graphicFiles.add(url);
            	return false;
        	}
            if(url.trim().toLowerCase().endsWith("xls")|| url.trim().toLowerCase().endsWith("xlsx")){
        		return false;
        	}
            
            Document htmlDocument = connection.get();
            
            if(!connection.response().contentType().contains("text/html") && !connection.response().contentType().contains("text/plain"))
            {
            	nonHtml.add(url);
                System.out.println("Retrieved something other than HTML "+url);
                return false;
            }

            if(!url.trim().toLowerCase().startsWith(rootUrl)){
            	String title = htmlDocument.title();
        		outGoingLinksMap.put(url, title);
        		return false;
        	}
            
            String htmlText = "";
            if(visited.containsKey(url)){
            	int count = visited.get(url);
            	visited.put(url, ++count);
            	return false;
            } else {
            	htmlText = htmlDocument.body().text();
            	visited.put(url, 1);
            }
            List<String> result = new ArrayList<>();
			htmlText = relaceSpecialCharsWithSpaces(htmlText);
            String[] wordsInHTML = htmlText.split(" ");
            for(String word: wordsInHTML) {
            	if(!StringUtil.isBlank(word)) {
					word = word.trim();
            		Stemmer stemmer = new Stemmer();
            		stemmer.add(word.toCharArray(),word.length());
                	stemmer.stem();
                	result.add(stemmer.toString());
            	}
            	
            }

			Map <String, Integer> wordMap = new HashMap<>();
            for(String stemWord : result){
	            if(wordMap.containsKey(stemWord)){
	            	int count = wordMap.get(stemWord);
	            	wordMap.put(stemWord, ++count);
	            } else {
	            	wordMap.put(stemWord, 1);
	            }
            }
			Statement stmt = conn.createStatement();
			wordMap.forEach((word, count) -> {

				String query = String.format("INSERT INTO word_url_count (word, url, count) VALUES ('%s', '%s', %d)",word,url.replace("https://",""),count);
				//System.out.println(query);
				try {
					stmt.execute(query);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});
            //result.add( url);
			listOfAllWords.addAll(result);

            if(countOfLinks>= maxDepthToCrawl){
            	System.out.println("Reached Max pages to Crawl "+ maxDepthToCrawl);
            	return true;
            }
            countOfLinks++;
            Elements linksOnPage = htmlDocument.select("a[href]");
            //System.out.println("Found (" + linksOnPage.size() + ") links");
            for(Element link : linksOnPage)
            {

            	//System.out.println(url);
                //this.links.add(link.absUrl("href"));
				String urlToBeCrawled = link.absUrl("href");
				if(urlToBeCrawled.contains("#")){
					urlToBeCrawled = urlToBeCrawled.replaceAll("#.*","");
				}
				if(urlToBeCrawled.endsWith("/")){
					urlToBeCrawled = urlToBeCrawled.substring(0,urlToBeCrawled.length()-1);
				}
                crawl(urlToBeCrawled);

            }
            return true;
        }catch(HttpStatusException e){
        	brokenUrl.add(url);
        	System.out.println("Invalid URL "+url);
        	return false;
        }
        catch(IOException ioe){
            // We were not successful in our HTTP request
        	System.out.println(url);
        	System.out.println(ioe);
            return false;
        } catch(IllegalArgumentException iae){
        	System.out.println(url);
        	iae.printStackTrace();
        	return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
        	//System.out.println(url);
        }
    }
    
    private String relaceSpecialCharsWithSpaces(String word) {
		System.out.println(word);
    	StringBuilder sb = new StringBuilder();
    	for(char ch: word.toCharArray()) {
    		if(Character.isAlphabetic(ch) || Character.isDigit(ch)) {
    			sb.append(ch);
    		} else {
				sb.append(" ");
			}
    	}
		System.out.println(sb);
		return sb.toString().toLowerCase();
 	   
    }
    
    public void parseRobots(String url){
			try{
				System.out.println();
				BufferedReader in = new BufferedReader(	
		            new InputStreamReader(new URL(url+"/robots.txt").openStream()));
		        String line = null;
		        while((line = in.readLine()) != null) {
		            if(!line.trim().startsWith("#")){
		            	if(line.trim().startsWith("Disallow")){
		            		disAllowed.add(url+line.trim().substring(line.trim().indexOf(':')+1).trim());
		            	}
		            }
		        }
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
    }
    public boolean isValidLink(String url){
    	for(String durl:disAllowed){
    		if(durl.equalsIgnoreCase(url)){
    			return false;
    		}
    	}
    	return true;
    }
    
    private boolean isExternalLink(String url) {
    	if(!url.toLowerCase().startsWith(rootUrl)) {
    		return true;
    	}
    	return false;
    }

    public List<String> getLinks()
    {
        return this.links;
    }

	public int getCountOfLinks() {
		return countOfLinks;
	}

	public Map<String, Integer> getVisited() {
		return visited;
	}

	public Set<String> getBrokenUrl() {
		return brokenUrl;
	}

	public Set<String> getNonHtml() {
		return nonHtml;
	}
	
	public Map<Integer, Set<String>> getWordDocuments(){
		return wordDocuments;
	}

	public Set<String> getGraphicFiles() {
		return graphicFiles;
	}

	public Map<String,String> getOutGoingLinksMap() {
		return outGoingLinksMap;
	}
	
	public Map<String,Integer> getWordMap() {
		return null;
	}    

}
