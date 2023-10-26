from flask import Flask
app=Flask(__name__) #__name__代表目前執行的模組

@app.route("/")
def home():
    return "Hello Flask"

@app.route("/test")
def test():
    return "This is test"

if __name__=="__main__":
    app.run()