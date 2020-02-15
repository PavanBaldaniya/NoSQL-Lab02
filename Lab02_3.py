#Question-3

myRDD = sc.textFile("/FileStore/tables/web_access_log.txt")


def ImageCount(s):
  img = {"jpeg","jpg","png","jif","jfif","jfi","gif","webp","tiff","tif","psd","raw","arw","cr2","nrw","k25","bmp","dib","heif","heic","ind","indd","indt","jp2","j2k","jpx","jpm","mj2"}
  jpg = {"jpeg","jpg","jif","jfif","jfi"}
  gif = {"gif"}
  ext = s.split(" ")[6].split("/")[-1].split(".")[-1]
  if(ext in img):
    if(ext in jpg):
      return 'JPG'
    elif ext in gif:
      return 'GIF'
    else:
      return 'Other'
  else:
    return ' '
  

  
ans = myRDD.filter(lambda x: len(x)>0).map(ImageCount).filter(lambda x: x!=' ').map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).sortByKey(True)

for x in ans.collect():
  print(x[0],x[1])


#Question-4
  
myRDD = sc.textFile("/FileStore/tables/web_access_log.txt")

def RequestCount(s):
  month = s.split(" ")[3].split("/")[1]
  year = s.split(" ")[3].split("/")[2].split(":")[0]
  size = s.split(" ")[9]
  date = year+"-"+month
  if(size=='-'):
    c = 0
  else:
    c = int(size)
  x = (date,c)
  return x

res = myRDD.filter(lambda x: len(x)>0).map(RequestCount).map(lambda x: (x[0],(x[1],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).sortByKey(True)

for x in res.collect():
    print(x[0],x[1][1],x[1][0])
    

#Question-5

myRDD = sc.textFile("/FileStore/tables/web_access_log.txt")

def ErrorCount(s):
    e=s.split()[8]
    m = {
        'Jan':1,
        'Feb':2,
        'Mar':3,
        'Apr':4,
        'May':5,
        'Jun':6,
        'Jul':7,
        'Aug':8,
        'Sep':9,
        'Oct':10,
        'Nov':11,
        'Dec':12
        }
    if e=="404":
       t1=s.split()[3].split("/")
       timestamp=str(t1[2][:4])+"-"+str(m[t1[1]])+"-"+str(t1[0][1:])+t1[2][4:]
       url=s.split()[10]
       return (timestamp,url[1:len(url)-1])
    else:
        return (' ',s.split()[10])
    
res = myRDD.filter(lambda x: len(x)>0).map(ErrorCount).filter(lambda x: x[0]!=' ')

for x in res.collect():
  print(x[0],x[1])
