'''
Created on Apr 6, 2014

@author: scott_jackson
'''
treegrid = """
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta name="keywords" content="jquery,ui,easy,easyui,web">
    <meta name="description" content="easyui help you build your web page easily!">
    <title>PhotoData Viewer</title>
    <link rel="stylesheet" type="text/css" href="/static/easyui/themes/default/easyui.css">
    <link rel="stylesheet" type="text/css" href="/static/easyui/themes/icon.css">
    <script type="text/javascript" src="/static/jquery.min.js"></script>
    <script type="text/javascript" src="/static/jquery.easyui.min.js"></script>
</head>
<body>
    <h1>TreeGrid</h1>
    
    <table id="test" title="Folder Browser" class="easyui-treegrid" style="width:1200px;height:600px"
            url="/data/treegrid_node"
            rownumbers="true"
            idField="id" treeField="name"
            >
        <thead>
            <tr>
                <th field="name" width="160">Name</th>
                <th field="size" width="60" align="right">Size</th>
                <th field="date" width="100">Modified Date</th>
            </tr>
        </thead>
    </table>
    
</body>
</html>
"""
from bottle import route, run, static_file, post
import dbtree

@route('/static/<path:path>')
def server_static(path):
    print "#1 serving up:",path
    return static_file(path, root='C:\Users\scott_jackson\git\PhotoManager\Photo\src\static')

@post('/data/<path>')
def server_data(path):
    return(db.show_node(path))

db = dbtree.photo_collection()
db.connect('photo_database', 'barney')
db.mark_duplicates("/home/shared/Photos/2006")

def main():
    @route('/')
    @route('/hello')
    def hello():
        #return "Hello World!"
        return treegrid

    run(host='localhost', port=8080, debug=True)

if __name__ == '__main__':
    main()