from pytube import YouTube
from pytube.exceptions import VideoUnavailable, RegexMatchError, PytubeError
import os

def download_youtube_video_optimized(url, output_path='.'):
    """
    優化後的下載函式，嘗試修復（或避免） HTTP 400 錯誤。
    """
    print(f"嘗試下載網址: {url}")
    try:
        yt = YouTube(url)
        print(f"影片標題: {yt.title}")
        
        # 嘗試 progressive（含音訊）最高解析度
        video_stream = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
        if video_stream is None:
            print("注意：未找到 progressive 串流，將改用最高畫質 stream（可能需額外處理音訊）。")
            video_stream = yt.streams.get_highest_resolution()
        
        if video_stream:
            print(f"選定解析度: {getattr(video_stream, 'resolution', 'N/A')}")
            print("開始下載…")
            # 確保路徑存在
            os.makedirs(output_path, exist_ok=True)
            video_stream.download(output_path=output_path)
            print(f"下載完成！影片已儲存至: {os.path.join(output_path, video_stream.default_filename)}")
        else:
            print("錯誤：找不到任何可下載的影片串流。")

    except VideoUnavailable:
        print("下載時發生錯誤: 影片不可用或連結失效。")
    except RegexMatchError:
        print("下載時發生錯誤: 無法解析 YouTube 網址。請檢查網址格式是否正確。")
    except PytubeError as e:
        print(f"PyTube 專屬錯誤: {e}")
        print("這可能是由於 pytube 版本過舊或 YouTube API 已變更。請執行 `pip install --upgrade pytube`。")
    except Exception as e:
        print(f"下載時發生其他錯誤: {e}")
        print("可能原因：YouTube 網頁結構變更、網路問題、或使用代理/封鎖。")


download_youtube_video_optimized("https://www.youtube.com/watch?v=2f0vVejDpv4&list=RD2f0vVejDpv4&start_radio=1", output_path='.')