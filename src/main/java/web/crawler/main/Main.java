package web.crawler.main;

import java.util.Map.Entry;


public class Main {

	/**
	 * This program takes two command line arguments
	 * 1. The website you want to crawl
	 * 2. Maximum depth you want to go to crawl
	 * @param args
	 */
	public static void main(String[] args) {
		String url = args[0];
		WebCrawler.rootUrl = url;
		WebCrawler.maxDepthToCrawl = Integer.parseInt(args[1]);
		int max = 0;
		WebCrawler webCrawler = new WebCrawler();
		webCrawler.parseRobots(url);
		webCrawler.crawl(url);
		webCrawler.printResults();

		System.out.print("All Visited Links in "+url+": ");
		for(Entry<String, Integer> entry : webCrawler.getVisited().entrySet()){
			System.out.print(entry.getKey()+" ");
		}
		System.out.println();
		System.out.println("Outgoing Links with the title: "+ webCrawler.getOutGoingLinksMap());
		

		System.out.println("Broken links: "+ webCrawler.getBrokenUrl());
		System.out.println("Graphic Files: "+ webCrawler.getGraphicFiles());
		System.out.println("Number of Graphic files  are "+ webCrawler.getGraphicFiles().size());
		
		//System.out.println("wordMap: "+spiderLeg.getWordMap());

    }

}
