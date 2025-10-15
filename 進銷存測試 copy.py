import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any

class OrderManager:
    """
    柏暉股份有限公司 印刷電路板生產通知單 訂單管理類別
    用於載入、新增、顯示和儲存訂單資料。
    """
    def __init__(self, excel_file='order_data.xlsx'):
        self.excel_file = excel_file
        self.data = []  # 儲存訂單資料的列表 (用於 DataFrame 重建)
        self.df = pd.DataFrame() # 訂單數據的 DataFrame
        self.columns = [] # 儲存欄位名稱
        self.initialize_dataframe()
        self.load_data()

    def load_data(self):
        """從 Excel 檔案載入現有資料。如果檔案不存在，則初始化一個空的 DataFrame。"""
        if os.path.exists(self.excel_file):
            try:
                self.df = pd.read_excel(self.excel_file, engine='openpyxl')
                self.data = self.df.replace({pd.NA: ''}).to_dict('records') # 載入時將 NaN 替換為空字串
                print(f"✅ 成功從 {self.excel_file} 載入 {len(self.data)} 筆訂單。")
            except Exception as e:
                print(f"⚠️ 載入 Excel 檔案時發生錯誤: {e}。將以空資料啟動。")
                self.data = []
                self.df = pd.DataFrame(columns=self.columns)
        else:
            print("🆕 找不到訂單檔案，已初始化訂單管理系統。")
            # 檔案不存在，保持 df 為初始化後的空 DataFrame

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

    def add_order(self, order_details: Dict[str, Any]):
        """
        新增一筆訂單資料。
        :param order_details: 包含訂單詳細資訊的字典。
        """
        # 確保字典中的鍵與欄位名稱一致，並填補缺失的欄位
        new_order = {col: order_details.get(col, '') for col in self.columns}
        
        # 將訂單序號格式化為適合顯示的字串
        order_seq_display = str(new_order.get('訂單序號', 'N/A'))
        
        self.data.append(new_order)
        # 從最新的 data 列表重建 df
        self.df = pd.DataFrame(self.data) 
        print(f"\n✨ 訂單 {order_seq_display} 新增成功！")

    def display_orders(self):
        """顯示目前所有訂單的摘要資訊。"""
        if self.df.empty:
            print("\n🚨 目前系統中沒有任何訂單資料。")
            return
        
        # 選擇主要欄位進行顯示
        display_cols = ["訂單序號", "客戶名稱", "訂單數量", "發單日", "交貨日期"]
        print("\n--- 所有訂單摘要 ---")
        # 使用 .fillna('') 避免在顯示時出現 NaN
        print(self.df[display_cols].fillna('').to_string(index=False)) 
        print("--------------------")

    def save_data(self):
        """將目前的所有訂單資料存入 Excel 檔案。"""
        try:
            # 確保儲存時，所有欄位都存在
            self.df.to_excel(self.excel_file, index=False, engine='openpyxl')
            print(f"\n💾 所有訂單資料已成功儲存到檔案：{self.excel_file}")
        except Exception as e:
            print(f"\n❌ 儲存資料到 Excel 時發生錯誤: {e}")

    # 1. 新增的功能：顯示目前資料筆數
    def show_count(self):
        """顯示目前資料筆數。"""
        count = len(self.df)
        print(f"📊 目前系統中有 {count} 筆訂單資料。")
        return count


# 2. 新增的功能：互動式輸入畫面
def get_user_input(manager: OrderManager) -> Optional[Dict[str, Any]]:
    """
    提供一個互動式介面，讓使用者輸入新的訂單詳細資訊。
    """
    print("\n" + "=" * 40)
    print("【新增訂單：請依序輸入資料】")
    print("=" * 40)
    
    # 建立一個包含所有欄位的字典，預設為空字串
    new_order: Dict[str, Any] = {col: '' for col in manager.columns}
    
    # --- 互動式輸入重要欄位 ---
    
    # 1. 訂單序號 (唯一識別，必填)
    while True:
        order_seq = input("1. 訂單序號 (例如 12.207，必填): ").strip()
        if order_seq:
            try:
                # 嘗試將其轉換為 float (因為範例是 12.207)
                new_order["訂單序號"] = float(order_seq)
                break
            except ValueError:
                print("❌ 訂單序號格式錯誤，請輸入數字 (例如 12.207)。")
        else:
            print("❌ 訂單序號為必填欄位。")
            return None # 缺少必填欄位則退出

    # 2. 客戶名稱 (必填)
    new_order["客戶名稱"] = input("2. 客戶名稱 (必填): ").strip()
    if not new_order["客戶名稱"]:
        print("❌ 客戶名稱為必填欄位。")
        return None

    # 3. 訂單數量
    while True:
        qty = input("3. 訂單數量 (輸入數字): ").strip()
        if not qty:
            new_order["訂單數量"] = 0 
            break
        try:
            new_order["訂單數量"] = int(qty)
            break
        except ValueError:
            print("❌ 訂單數量請輸入有效的整數。")

    # 4. 發單日
    default_date = datetime.now().strftime('%Y/%m/%d')
    new_order["發單日"] = input(f"4. 發單日 (預設: {default_date}): ").strip() or default_date
    
    # 5. 交貨日期
    new_order["交貨日期"] = input("5. 交貨日期 (例如 2026/1/5): ").strip()
    
    # 6. 廠內料號
    new_order["廠內料號"] = input("6. 廠內料號: ").strip()
    
    
    # --- 其他次要欄位，讓使用者選擇是否要輸入 ---
    print("\n" + "~" * 40)
    print("【其他次要欄位輸入 (可直接按 Enter 跳過)】")
    print("~" * 40)
    
    # 輸入所有剩餘欄位
    for col in manager.columns:
        # 跳過已經問過的重要欄位
        if col in ["訂單序號", "客戶名稱", "訂單數量", "發單日", "交貨日期", "廠內料號"]:
            continue
            
        prompt = f"   - {col} (目前值: {new_order.get(col, '')}): "
        user_input = input(prompt).strip()
        if user_input:
            new_order[col] = user_input # 使用者輸入的值
        # 如果使用者沒有輸入，則保留初始化時的空字串

    return new_order


def main_menu():
    """應用程式的主菜單循環。"""
    manager = OrderManager()
    
    while True:
        print("\n" + "=" * 50)
        print("【柏暉股份有限公司：印刷電路板訂單管理系統】")
        manager.show_count() # 顯示目前資料筆數
        print("-" * 50)
        print("請選擇操作：")
        print("1. 🆕 新增訂單資料 (進入輸入畫面)")
        print("2. 📄 顯示所有訂單摘要")
        print("3. 💾 儲存資料並退出系統")
        print("4. ❌ 僅退出 (不儲存最新變更)")
        print("=" * 50)
        
        choice = input("請輸入您的選擇 (1-4): ").strip()
        
        if choice == '1':
            order_details = get_user_input(manager)
            
            # 只有當訂單資料有效時才新增
            if order_details is not None:
                manager.add_order(order_details)
                
        elif choice == '2':
            manager.display_orders()
            
        elif choice == '3':
            manager.save_data()
            print("系統已退出。感謝您的使用！")
            break
            
        elif choice == '4':
            print("系統已退出 (未儲存最新變更)。")
            break
            
        else:
            print("⚠️ 輸入錯誤，請輸入 1 到 4 之間的數字。")


if __name__ == '__main__':
    main_menu()
