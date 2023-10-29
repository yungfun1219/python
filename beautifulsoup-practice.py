import requests
url = "https://www.ptt.cc/bbs/movie/index.html"
requestHtml = requests.request(url, headers={
    "User-Aent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
})
with requestHtml.urlopen(requestHtml) as response:
    data = response.read().decode("utf-8")
    
print(data)
