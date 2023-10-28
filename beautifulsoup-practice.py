import urllib.request as req
url = "https://www.ptt.cc/bbs/movie/index.html"
request = req.Request(url, headers={
    "User-Aent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
})
with req.urlopen(request) as response:
    data = response.read().decode("utf-8")
    
print(data)
