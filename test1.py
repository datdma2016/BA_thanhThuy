from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Xin chao BA Thanh Thuy! He thong da hoat dong.'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
