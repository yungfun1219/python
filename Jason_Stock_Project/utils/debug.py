import inspect
import sys

# 獲取並返回當前程式碼執行的行號。
def line_number():
    """
    注意：在某些環境中 (如某些 IDE 或 Jupyternotebook)，
    inspect.currentframe() 可能會返回 None，因此需要處理這個情況。
    --- 在要顯示出來degub的地方加入，即可在執行時顯示，記得import模組。
    import utils.debug as debug
    print(f"這行程式碼的行號是: {debug.line_number()}") # 行號: N
    ---
    """
    try:
        # sys._getframe() 是一個更快且更可靠的方法，但在某些受限環境中可能不可用。
        # 0 表示當前函式堆疊，1 表示調用它的函式堆疊。
        # 因為我們在一個輔助函式內，所以使用 1 獲取調用 line_number() 的位置。
        return sys._getframe(1).f_lineno 
    except AttributeError:
        # 如果 sys._getframe 不可用，則回退到 inspect 模組
        # currentframe() 獲取當前的堆疊幀
        frame = inspect.currentframe()
        # f_lineno 屬性包含該幀的行號
        # 由於當前幀是 line_number() 內部，我們需要獲取前一幀 (f_back)
        if frame:
            return frame.f_back.f_lineno if frame.f_back else "無法獲取"
        return "無法獲取"
# ---------------------------------------------
