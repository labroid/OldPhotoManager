import os, json
import cherrypy

#TODO:  Serve .js locally so this works w/o internet connection

# Our EasyUI HTML
EASYGRIDUI_HEADER = """
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta name="keywords" content="jquery,ui,easy,easyui,web">
    <meta name="description" content="easyui help you build your web page easily!">
    <title>Duplicate Photo Finder</title>
    <link rel="stylesheet" type="text/css" href="http://www.jeasyui.com/easyui/themes/default/easyui.css">
    <link rel="stylesheet" type="text/css" href="http://www.jeasyui.com/easyui/themes/icon.css">
    <link rel="stylesheet" type="text/css" href="http://www.jeasyui.com/easyui/demo/demo.css">
    <script type="text/javascript" src="http://code.jquery.com/jquery-1.6.1.min.js"></script>
    <script type="text/javascript" src="http://www.jeasyui.com/easyui/jquery.easyui.min.js"></script>
</head>
"""

EASYGRIDUI_PAGETOP = """
<body>
    <h2>Duplicate Photo Finder</h2>
"""

EASYGRIDUI_TREEGRID = """    
    <table title="Photos" class="easyui-treegrid" style="width:700px;height:300px"
            url="/treegrid_json"
            rownumbers="true"
            idField="id" treeField="name">
        <thead>
            <tr>
                <th field="path" width="250">Path</th>
                <th field="size" width="100" align="right">Size</th>
                <th field="MD5" width="150" align="right" formatter="formatDollar">MD5</th>
                <th field="AllIn" width="150" align="right" formatter="formatDollar">All In</th>
                <th field="NoneIn" width="150" align="right" formatter="formatDollar">None In</th>
                <th field="Candidates" width="150" align="left">Candidates</th>
            </tr>
        </thead>
    </table>
    <script>
        function formatDollar(value){
            if (value){
                return '$'+value;
            } else {
                return '';
            }
        }
    </script>
"""
    
EASYGRIDUI_CLOSE = """
</body>
</html>
"""
seed_response = """
[{
    "id":1,
    "name":"C",
    "size":"",
    "date":"02/19/2010",
    "state":"closed",
    "children":[{
        "id":2,
        "name":"Program Files",
        "size":"120 MB",
        "date":"03/20/2010",
        "children":[{
            "id":21,
            "name":"Java",
            "size":"",
            "date":"01/13/2010",
            "state":"closed",
            "children":[{
                "id":211,
                "name":"java.exe",
                "size":"142 KB",
                "date":"01/13/2010"
            },{
                "id":212,
                "name":"jawt.dll",
                "size":"5 KB",
                "date":"01/13/2010"
            }]
        },{
            "id":22,
            "name":"MySQL",
            "size":"",
            "date":"01/13/2010",
            "state":"closed",
            "children":[{
                "id":221,
                "name":"my.ini",
                "size":"10 KB",
                "date":"02/26/2009"
            },{
                "id":222,
                "name":"my-huge.ini",
                "size":"5 KB",
                "date":"02/26/2009"
            },{
                "id":223,
                "name":"my-large.ini",
                "size":"5 KB",
                "date":"02/26/2009"
            }]
        }]
    },{
        "id":3,
        "name":"eclipse",
        "size":"",
        "date":"01/20/2010",
        "state":"closed"
    }]
}]
"""
expand_response = """
[
    {
        "id":31,
        "parentId": "X",
        "name":"eclipse.exe",
        "size":"56 KB",
        "date":"05/19/2009"
    },{
        "id":32,
        "parentId": "X",
        "name":"eclipse.ini",
        "size":"1 KB",
        "date":"04/20/2010"
    },{
        "id":33,
        "parentId": "X",
        "name":"notice.html",
        "size":"7 KB",
        "date":"03/17/2005"
    }
]
        """
def build_data_dictionary():
    pass

# Now for the generic CherryPy app
class jqGrid(object):
    """Our CherryPy application class"""
    @cherrypy.expose
    def index(self):
        """Serve up the page that will render the grid"""
        return EASYGRIDUI_HEADER + EASYGRIDUI_PAGETOP + EASYGRIDUI_TREEGRID + EASYGRIDUI_CLOSE

    @cherrypy.expose
    def treegrid_json(self, id = None): # 1 line
        if id is None:
            return(seed_response)
        else:
            return(expand_response)
#        return jqgrid_json(self, users_list, header, rows=rows, sidx=sidx, _search=_search,
#        searchField=searchField, searchOper=searchOper, searchString=searchString, page=page, sord=sord) # 5 lines!

conf = {
        '/': {
            'tools.staticdir.on': True,
            'tools.staticdir.root': os.getcwd(),
            'tools.staticdir.dir': ''
        },
    }
app = cherrypy.tree.mount(jqGrid(), config=conf)
cherrypy.quickstart(app)