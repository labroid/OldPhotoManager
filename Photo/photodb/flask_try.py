from flask import Flask, render_template, request, make_response
from flask_restful import Resource, Api, reqparse
import photo_data

ARCHIVE_HOSTS = ['localhost', 'barney', 'smithers', 'google', 'other']  #First will be default
ARCHIVE_DEFAULT_REPO = 'barney'
ARCHIVE_DEFAULT_TOP = 'root'
TARGET_HOST = ['localhost', 'barney', 'smithers', 'google', 'other']  #First will be default
TARGET_DEFAULT_REPO = 'barney'
TARGET_DEFAULT_TOP = 'root'


archive_host_str = "archive_host"
archive_respository_str = "archive_repository"
archive_top_str = "archive_top"
archive_hosts_str = "archive_hosts"
archive_repositories_str = "archive_repositories"

app = Flask(__name__)
api = Api(app)

@app.route('/')
def root():
    return make_response(open('templates/dropdown.html').read())  #Maybe change make_response to send_file in production as send_file is cached in the client

#Browser state contains:
#   archive_host
#   archive_repository
#   archive_top
#   archive_status
#   target_host
#   target_repository
#   target_top

class TestJson(Resource):
    def post(self):
        x = {}
        x['host'] = 'localhost'
        x['hosts'] = [
            'localhost',
            'barney',
            'smithers',
            'google',
            'other'
        ]
        x['repo'] = 'barney'
        x['repos'] = [
            'smithers',
            'barney',
            'test'
        ]
        return(x)

class SetArchive(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument(archive_host_str, type=str)
        parser.add_argument(archive_respository_str, type=str)
        parser.add_argument(archive_top_str, type=str)
        args = parser.parse_args()

        print args
        if args[archive_host_str] is None:
            archive_host = ARCHIVE_HOSTS[0]
            archive_hosts = ARCHIVE_HOSTS
            archive_top = ARCHIVE_DEFAULT_TOP
        else:
            archive_host = args[archive_host_str]
            archive_repository = args[archive_respository_str]
            archive_top = args[archive_top_str]

        archive_status, archive_repositories, archive_message = photo_data.check_host_get_repositories(archive_host)

        #     Create a cursor instance to selected host
        #     if successful:
        #         retreive repositories
        #         retreive tops
        #         return archive_host, archive_hosts, archive_repo, archive_repos, archive_top, archive_tops, archive_status, repo_count, top_count
        #     else
        #         host not available
        return({archive_host_str: archive_host, archive_hosts_str: archive_hosts, archive_repositories_str: archive_repositories, archive_top_str: archive_top})



api.add_resource(TestJson, '/test_json')

if __name__ == '__main__':
    app.run(debug=True)