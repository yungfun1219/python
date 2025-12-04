from pathlib import Path

BASE_DIR = Path("D:/Python_repo/python/Jason_Stock_Project/datas/raw/1_STOCK_DAY")

def delete_empty_folders(base_path: Path):
    # 由最深層開始檢查，避免父資料夾刪除後子資料夾尚未處理
    for folder in sorted(base_path.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if folder.is_dir():
            try:
                if not any(folder.iterdir()):  # 空的資料夾
                    folder.rmdir()
                    print(f"已刪除空資料夾：{folder}")
            except Exception as e:
                print(f"刪除失敗 {folder}: {e}")

# 執行
delete_empty_folders(BASE_DIR)
print("✔️ 完成空資料夾清理")
