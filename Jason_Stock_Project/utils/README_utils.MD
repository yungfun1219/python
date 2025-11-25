Python 實用工具函式集 (Utility Functions)

1. transform_date(date: str) -> str
   將中華民國紀年日期轉換為西元紀年。

2. get_path_to_folder_file(file_path_str: str) -> tuple[str, str]
   從檔案的完整路徑中，分離出資料夾路徑和檔案名稱。

3. check_and_create_folder(folder_path: str | Path) -> bool
   檢查指定的資料夾是否存在。若不存在，則遞迴建立資料夾 (包含所有父資料夾)。
   成功或已存在回傳 True，否則回傳 False。
4. check_file_exists(file_path: str | Path) -> bool
   檢查指定路徑是否確實存在一個檔案 (而非資料夾)。
5. get_extracted_string(file_path: str, target_prefix: str) -> Optional[str]
   從指定的日誌檔案內容中，使用正規表達式擷取特定前綴後面的第一個數字字串。
