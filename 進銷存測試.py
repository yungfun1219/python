import pandas as pd
import os
from datetime import datetime

class OrderManager:
    """
    柏暉股份有限公司 印刷電路板生產通知單 訂單管理類別
    根據提供的圖片欄位設計數據結構。
    """
    def __init__(self, excel_file='order_data.xlsx'):
        self.excel_file = excel_file
        self.data = []  # 儲存訂單資料的列表
        self.load_data()

    def load_data(self):
        """從 Excel 檔案載入現有資料。如果檔案不存在，則初始化一個空的 DataFrame。"""
        if os.path.exists(self.excel_file):
            try:
                self.df = pd.read_excel(self.excel_file)
                self.data = self.df.to_dict('records')
                print(f"✅ 成功從 {self.excel_file} 載入 {len(self.data)} 筆訂單。")
            except Exception as e:
                print(f"⚠️ 載入 Excel 檔案時發生錯誤: {e}。將以空資料啟動。")
                self.initialize_dataframe()
        else:
            self.initialize_dataframe()
            print("🆕 找不到訂單檔案，已初始化訂單管理系統。")

    def initialize_dataframe(self):
        """初始化空的 Pandas DataFrame，定義所有欄位。"""
        self.columns = [
            "訂單序號", "發單日", "訂單編號", "廠內料號", "客戶名稱", 
            "製作", "板材廠牌", "發料尺寸", "防焊", "表面處理", 
            "板邊處理", "單片尺寸", "出貨尺寸", "特殊要求", "備註", 
            "熱導係數", "銅箔厚度", "文字", "基板厚度", "排版片數", 
            "共模料號", "底片編號", "交貨日期", "訂單數量", "最低需求量", 
            "確認", "承認書", "底片更新"
        ]
        self.df = pd.DataFrame(columns=self.columns)

    def add_order(self, order_details):
        """
        新增一筆訂單資料。
        :param order_details: 包含訂單詳細資訊的字典。
        """
        # 確保字典中的鍵與欄位名稱一致
        new_order = {col: order_details.get(col, '') for col in self.columns}
        
        self.data.append(new_order)
        self.df = pd.DataFrame(self.data)
        print(f"\n✨ 訂單 {new_order.get('訂單序號', 'N/A')} 新增成功！")

    def display_orders(self):
        """顯示目前所有訂單的摘要資訊。"""
        if self.df.empty:
            print("\n🚨 目前系統中沒有任何訂單資料。")
            return
        
        # 選擇主要欄位進行顯示
        display_cols = ["訂單序號", "客戶名稱", "訂單數量", "發單日", "交貨日期"]
        print("\n--- 所有訂單摘要 ---")
        print(self.df[display_cols].to_string(index=False))
        print("--------------------")

    def save_data(self):
        """將目前的所有訂單資料存入 Excel 檔案。"""
        try:
            self.df.to_excel(self.excel_file, index=False, engine='openpyxl')
            print(f"\n💾 所有訂單資料已成功儲存到檔案：{self.excel_file}")
        except Exception as e:
            print(f"\n❌ 儲存資料到 Excel 時發生錯誤: {e}")

# --- 示範使用 ---

# 1. 初始化訂單管理器
manager = OrderManager()

# 2. 準備範例訂單數據 (根據您的圖片填寫)
# 注意：日期格式建議使用 YYYY/MM/DD
sample_order_1 = {
    "訂單序號": 12.207,
    "發單日": "2025/10/15",
    "訂單編號": "1331-20251014002",
    "廠內料號": "KT-277-0007B",
    "客戶名稱": "PCFTZP2-01.0 / FZT 前燈 LED燈板 V2.0",
    "製作": "量產",
    "板材廠牌": "騰輝VT-4B5(無陽極)(50um)",
    "發料尺寸": "510x610mm",
    "防焊": "亮面黑 R500",
    "表面處理": "OSP",
    "板邊處理": "沖 NC V-cut",
    "單片尺寸": "30.2 x 22.85mm",
    "出貨尺寸": "154.8 x 136.26mm",
    "特殊要求": "★特殊要求←福安★\n1. 樣品製作，拆燙加手改週期\n...\n4. 20200814 開模具(模具費用128,000元，客戶負擔)\n5. 20210712 續單，開啟具測試",
    "備註": "折燙于改週期 2550(兩批一起生產，此為第二批)\n開啟治具測試不需AOI\n福安皆需特殊內標籤",
    "熱導係數": "5W",
    "銅箔厚度": "1 oz",
    "文字": "白色",
    "基板厚度": "1.6mm",
    "排版片數": "3 x 4 x 20 = 240",
    "共模料號": "KT-277-0007B",
    "底片編號": "UL印刷：文字層",
    "交貨日期": "2026/1/5",
    "訂單數量": 7500,
    "最低需求量": "SHT",
    "確認": "完成",
    "承認書": "完成",
    "底片更新": "週期更新"
}

# 3. 新增訂單
manager.add_order(sample_order_1)

# 4. 顯示所有訂單
manager.display_orders()

# 5. 儲存數據
manager.save_data()