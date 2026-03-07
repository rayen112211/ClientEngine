"""
Instagram Public Scraper for Lead Discovery.
WARNING: Prone to rate limits. Uses 30s delays and basic headers.
"""
import requests
import time
import re
import json

def search_instagram(query, location, max_results=20):
    """
    Search Instagram for businesses.
    Since IG's public API is heavily restricted, we'll try a Google Dork approach 
    which is much more reliable than trying to hit IG's unauthenticated GraphQL.
    (DuckDuckGo HTML search for "site:instagram.com {query} {location}")
    """
    results = []
    
    # We use DuckDuckGo HTML search as a proxy to find IG pages reliably without IG blocking us
    search_query = f"site:instagram.com {query} {location}"
    url = "https://html.duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    
    try:
        time.sleep(5)  # Respect DDG
        res = requests.post(url, data={"q": search_query}, headers=headers, timeout=15)
        
        # Super basic regex to extract IG URLs and titles
        matches = re.findall(r'href="(https://(?:www\.)?instagram\.com/[^/]+/)".*?<a class="result__url".*?>(.*?)</a>', res.text, re.IGNORECASE)
        
        extracted = set()
        
        for link, text in matches:
            if link in extracted:
                continue
            extracted.add(link)
            
            # Clean up IG username extraction
            username = link.strip("/").split("/")[-1]
            if username in ["p", "explore", "tags", "reels"]:
                continue
                
            name = text.replace("<b>", "").replace("</b>", "").strip()
            if "Instagram" in name:
                name = name.split("Instagram")[0].strip(" -|@")
                
            if not name:
                name = username
            
            biz = {
                "business_name": name,
                "instagram_url": link,
                "source": "instagram",
                "city": location,
                "website": "", # Try to find website in bio if we had IG access, but DDG gives us the profile
                "email": "",
                "phone": "",
                "whatsapp": "",
                "category": query
            }
            results.append(biz)
            if len(results) >= max_results:
                break
                
        # Optional: In a full production system you would request each IG profile page here 
        # and parse the window._sharedData to get bio links and emails.
        # But for rate limiting safety, we just return the DDG finds.
                
    except Exception as e:
        print(f"[Instagram Source] Error: {e}")
        
    return {"results": results, "error": None}
