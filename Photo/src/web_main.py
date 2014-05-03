'''
Created on Oct 18, 2013

@author: scott_jackson
'''
#!python
import os.path
current_dir = os.path.dirname(os.path.abspath(__file__))

import cherrypy


class Root:
    @cherrypy.expose
    def index(self):
        return """
        <html>
            <head>
                <title>Photo Duplicate Tool</title>
                <script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
                <script src="scripts/jquery.jstree.js">
                <script type="text/javascript">
                    $(document).ready(function () {
                        $("#demo1").jstree({ 
                            "json_data" : {
                                "data" : [
                                    { 
                                        "data" : "A node",
                                        "children" : [ "Child 1", "A Child 2" ]
                                    },
                                    { 
                                        "attr" : { "id" : "li.node.id1" }, 
                                        "data" : { 
                                            "title" : "Long format demo"
                                        } 
                                    }
                                ]
                            },
                            "plugins" : ["themes", "json_data", "ui" ]
                        }).bind("select_node.jstree", function (e, data) { alert(data.rslt.obj.data("id")); });
                    });
                </script>
            </head>
            <body>
                <p>Hello</p>
                <div id="demo1">
                </div>
                <p>Goodbye</p>
            </body>
        </html>
        """

cherrypy.quickstart(Root(), script_name='', config='web.conf')
