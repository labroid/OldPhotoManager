from flask import Flask

    app = Flask(__name__)

    @app.route('/')
    def root():
        page = '''
        <!DOCTYPE html>
        <html>
        <head>
          <meta http-equiv="content-type" content="text/html; charset=ISO-8859-1">

          <script src="../static/lib/jquery.js" type="text/javascript"></script>
          <script src="../static/lib/jquery-ui.custom.js" type="text/javascript"></script>

          <link href="../static/src/skin-win8/ui.fancytree.css" rel="stylesheet" type="text/css">
          <script src="../static/src/jquery.fancytree.js" type="text/javascript"></script>

          <script type="text/javascript">
            $(function(){
              $("#tree").fancytree({
              url: "/db",
              cache: false
              });
            });
          </script>
        </head>

        <body>
          <div id="tree" data-source="ajax">
          </div>
        </body>
        </html>
        '''
        return page

    @app.route('/db')
    def db():
        json_output = '''
        [
      {
        "folder": true,
        "key": "Folder 1",
        "title": "Folder 1"
      },
      [
        {
          "folder": true,
          "key": "Folder 2",
          "title": "Folder 2"
        },
        {
          "folder": true,
          "key": "Folder 3",
          "title": "Folder 3"
        }
      ]
    ]
        '''
        return json_output

    if __name__ == '__main__':
        app.run()