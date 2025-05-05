from fastapi import FastAPI, Request, Response, HTTPException, Query
from fastapi.responses import StreamingResponse
import httpx
from urllib.parse import urljoin, urlparse
import logging
import asyncio
from typing import Optional, Dict, Any
import re

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("hls_proxy")

app = FastAPI(title="High-Performance HLS Proxy")

# Configure client with reasonable timeouts and connection pool
client = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0),
    follow_redirects=True,
    limits=httpx.Limits(max_keepalive_connections=100, max_connections=200)
)

@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()

async def fetch_with_referer(url: str, referer: Optional[str] = None, 
                            original_headers: Optional[Dict[str, str]] = None,
                            method: str = "GET", data: Any = None) -> httpx.Response:
    """Fetch a URL with custom referer and headers."""
    headers = {}
    
    # Copy original headers if provided
    if original_headers:
        # Filter out headers that might cause issues
        for k, v in original_headers.items():
            if k.lower() not in ['host', 'connection', 'content-length', 'content-encoding']:
                headers[k] = v
    
    # Add or override referer if provided
    if referer:
        headers['Referer'] = referer
        
    # Common headers for streaming content
    headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': urlparse(referer).netloc if referer else None,
        'Connection': 'keep-alive',
    })
    
    # Remove None values
    headers = {k: v for k, v in headers.items() if v is not None}
    
    try:
        if method.upper() == "GET":
            response = await client.get(url, headers=headers)
        elif method.upper() == "POST":
            response = await client.post(url, headers=headers, data=data)
        else:
            response = await client.request(method, url, headers=headers, data=data)
        
        return response
    except httpx.RequestError as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        raise HTTPException(status_code=502, detail=f"Error proxying request: {str(e)}")

def rewrite_m3u8_content(content: str, base_url: str, proxy_path: str, 
                         referer: Optional[str] = None) -> str:
    """
    Rewrite URLs in M3U8 content to be proxied through this service.
    """
    parsed_base = urlparse(base_url)
    base_scheme = parsed_base.scheme
    base_netloc = parsed_base.netloc
    
    # Function to transform URLs
    def rewrite_url(match):
        url = match.group(1)
        
        # Skip URLs that are already absolute
        if url.startswith('http://') or url.startswith('https://'):
            absolute_url = url
        # Handle absolute paths
        elif url.startswith('/'):
            absolute_url = f"{base_scheme}://{base_netloc}{url}"
        # Handle relative paths
        else:
            absolute_url = urljoin(base_url, url)
            
        # Encode the URL and referer for our proxy
        proxy_url = f"{proxy_path}?url={absolute_url}"
        if referer:
            proxy_url += f"&referer={referer}"
            
        return match.group(0).replace(url, proxy_url)
    
    # Replace URLs in different M3U8 directives
    patterns = [
        r'#EXT-X-STREAM-INF:[^\n]*\n([^\n#]+)',       # Variant streams
        r'#EXTINF:[^\n]*\n([^\n#]+)',                 # Segments
        r'#EXT-X-MAP:URI="([^"]+)"',                  # MAP directive
        r'#EXT-X-MEDIA:.*URI="([^"]+)"',              # Media playlists
        r'#EXT-X-I-FRAME-STREAM-INF:.*URI="([^"]+)"', # I-Frame playlists
    ]
    
    result = content
    for pattern in patterns:
        result = re.sub(pattern, rewrite_url, result)
        
    return result

@app.get("/proxy")
async def proxy_hls(request: Request, 
                   url: str = Query(..., description="HLS URL to proxy"),
                   referer: Optional[str] = Query(None, description="Referer to use")):
    """
    Proxy an HLS stream with the given URL and optional referer.
    """
    try:
        # Get headers from the client request
        original_headers = dict(request.headers)
        
        # Fetch the content
        response = await fetch_with_referer(url, referer, original_headers)
        
        # Check if it's an M3U8 file by content type or extension
        is_m3u8 = (
            'application/vnd.apple.mpegurl' in response.headers.get('content-type', '').lower() or
            'application/x-mpegurl' in response.headers.get('content-type', '').lower() or
            url.endswith('.m3u8')
        )
        
        # Pass through most headers from the source response
        headers = {}
        for key, value in response.headers.items():
            if key.lower() not in ['transfer-encoding', 'content-encoding', 'content-length']:
                headers[key] = value
        
        # Set CORS headers to allow requests from any origin
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
        headers['Access-Control-Allow-Headers'] = '*'
                
        if is_m3u8:
            # For M3U8 files, rewrite internal URLs to point back to our proxy
            content = response.text
            
            # Get the base URL for resolving relative paths
            base_url = url.rsplit('/', 1)[0] + '/' if '/' in url else url
            
            # Rewrite the content
            proxy_path = str(request.url).split('?')[0]  # Get the base proxy path
            rewritten_content = rewrite_m3u8_content(content, base_url, proxy_path, referer)
            
            # Return the modified content
            return Response(
                content=rewritten_content,
                headers=headers,
                status_code=response.status_code
            )
        else:
            # For non-M3U8 files (segments, etc.), stream the response directly
            async def stream_response():
                async for chunk in response.aiter_bytes():
                    yield chunk
                    
            return StreamingResponse(
                stream_response(),
                headers=headers,
                status_code=response.status_code
            )
            
    except Exception as e:
        logger.error(f"Error in proxy: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")

@app.get("/")
def read_root():
    """Root endpoint with usage instructions."""
    return {
        "message": "HLS Proxy Service",
        "usage": "GET /proxy?url=<HLS_URL>&referer=<REFERER>",
        "description": "Proxies HLS streams with optional referer support"
    }

@app.options("/proxy")
async def options_handler():
    """Handle CORS preflight requests."""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Max-Age": "86400",  # 24 hours
    }
    return Response(headers=headers)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, workers=4)
