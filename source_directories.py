"""
Free Directories Scraper (Yelp/YellowPages approximation via web search)
"""
import requests
import time
import re

def search_directories(query, location, max_results=30):
    """
    Search free business directories.
    Instead of writing 10 custom scrapers that break constantly, 
    we use a broad query directed at lists and directories.
    """
    results = []
    
    # We search general web for businesses matching the query to find ones in Yelp/YellowPages 
    # but we will extract the direct business URL if possible.
    search_query = f"{query} {location} (site:yelp.com OR site:yellowpages.com)"
    url = "https://html.duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
    }
    
    try:
        time.sleep(5) 
        res = requests.post(url, data={"q": search_query}, headers=headers, timeout=15)
        
        # Find business pages
        matches = re.findall(r'href="(https://[^"]+)".*?<a class="result__url".*?>(.*?)</a>', res.text, re.IGNORECASE)
        
        extracted = set()
        for link, text in matches:
            if link in extracted or "search?" in link or "/search/" in link:
                continue
            extracted.add(link)
            
            name = text.replace("<b>", "").replace("</b>", "").strip()
            name = name.split("-")[0].split("|")[0].strip()
            
            biz = {
                "business_name": name,
                "source": "directory",  # Will track it came from Yelp/YP
                "city": location,
                "website": "",
                "email": "",
                "phone": "",
                "category": query
            }
            results.append(biz)
            if len(results) >= max_results:
                break
                
    except Exception as e:
        print(f"[Directories Source] Error: {e}")
        
    return {"results": results, "error": None}
