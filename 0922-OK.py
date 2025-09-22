import requests
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stock_id = "2330"
url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
data = requests.get(url, verify=False).json()

df = pd.DataFrame(data["data"], columns=data["fields"])
print(df)

df.to_csv("D:\\Python_repo\\python\\Stock_day_data\\" + stock_id +"_202509.csv", index=False, encoding="big5")