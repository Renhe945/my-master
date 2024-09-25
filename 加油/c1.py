from  urllib.request import urlopen
url = "https://movie.douban.com/top250"
resp=urlopen(url)#打开网址
print(resp.read())#读取内容


