from flask import Flask,render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload',methods=['GET'])
def upload():
    return render_template('upload.html')

@app.route('/success',methods=['POST'])
def success():
    #<<<<---------mysql.py to load the data into SQL ----->>>
    return render_template('success.html')