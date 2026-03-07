"""
Facebook Public Scraper for Lead Discovery.
"""
import requests
import time
import re

def search_facebook(query, location, max_results=20):
    """
    Search Facebook for local businesses.
    Uses DDG proxy to avoid FB login walls.
    """
    results = []
    
    search_query = f"site:facebook.com {query} {location} -site:facebook.com/groups -site:facebook.com/events"
    url = "https://html.duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    
    try:
        time.sleep(5) 
        res = requests.post(url, data={"q": search_query}, headers=headers, timeout=15)
        
        matches = re.findall(r'href="(https://(?:www\.)?facebook\.com/[^/]+/)".*?<a class="result__url".*?>(.*?)</a>', res.text, re.IGNORECASE)
        
        extracted = set()
        
        for link, text in matches:
            if link in extracted:
                continue
            extracted.add(link)
            
            if "login.php" in link or "/public/" in link or "/places/" in link:
                continue
                
            name = text.replace("<b>", "").replace("</b>", "").strip()
            name = name.split("|")[0].split("-")[0].strip()
            
            biz = {
                "business_name": name,
                "facebook_url": link,
                "source": "facebook",
                "city": location,
                "website": "",
                "email": "",
                "phone": "",
                "whatsapp": "",
                "category": query
            }
            results.append(biz)
            if len(results) >= max_results:
                break
                
    except Exception as e:
        print(f"[Facebook Source] Error: {e}")
        
    return {"results": results, "error": None}
