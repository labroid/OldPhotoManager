from flask import Flask, render_template, request, make_response
from flask_restful import Resource, Api, reqparse
import photo_data

ARCHIVE_HOSTS = ['localhost', 'barney', 'smithers', 'google', 'other']  #First will be default
ARCHIVE_DEFAULT_REPO = 'barney'
ARCHIVE_DEFAULT_TOP = 'root'
TARGET_HOST = ['localhost', 'barney', 'smithers', 'google', 'other']  #First will be default
TARGET_DEFAULT_REPO = 'barney'
TARGET_DEFAULT_TOP = 'root'

host_str = "host"
repo_str = "repo"
repos_str = "repos"
top_str = "top"
tops_str = "tops"
hosts_str = "hosts"
host_status_str = "host_status"
repo_status_str = "repo_status"
top_status_str = "top_status"


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

class SetCollection(Resource):
    """
    Take request containing optional host, repo, top, return best we can know of host, host status, repos, repo, repo status, tops, top, top status
    """
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument(host_str, type=str)
        parser.add_argument(repo_str, type=str)
        parser.add_argument(top_str, type=str)
        args = parser.parse_args()

        host = None
        repo = None
        repos = None
        tops = None
        top = None
        host_status = None
        repo_status = None
        top_status = None

        collection_stats = {
            host_str: host,
            repo_str: repo,
            repos_str: repos,
            top_str: top,
            tops_str: tops,
            host_status_str: host_status,
            repo_status_str: repo_status,
            top_status_str: top_status
        }

        print "Args = {}".format(args)

        #If no host specified, just return all Nones
        if args[host_str] is None:
            return collection_stats

        #Check database for availability
        host = args[host_str]
        collection_stats[host_str] = host
        client, collection_stats[host_status_str] = photo_data.check_db_available(host)
        if client is None:
            return collection_stats

        repo = args[repo_str]
        repos, status = photo_data.get_db_collections(client)
        collection_stats[repos_str] = repos
        if repo is None:
            return collection_stats
        if repo in repos:
            collection_stats[repo_status_str] = "OK"
        else:
            collection_stats[repo_status_str] = "{} not in set {}".format(repo, repos)
            return collection_stats

        top = args[repo_str]
        tops, status = photo_data.get_db_tops(client, repo)
        collection_stats[tops_str] = tops
        if top is None:
            return collection_stats
        if top in tops:
            collection_stats[top_status_str] = "OK"
        else:
            collection_stats[repo_status_str] = "{} not in set {}".format(top, tops)
            return collection_stats

# class SetArchive(Resource):
#     def post(self):
#         parser = reqparse.RequestParser()
#         parser.add_argument(archive_host_str, type=str)
#         parser.add_argument(archive_respository_str, type=str)
#         parser.add_argument(archive_top_str, type=str)
#         args = parser.parse_args()
#
#         print args
#         if args[archive_host_str] is None:
#             archive_host = ARCHIVE_HOSTS[0]
#             archive_hosts = ARCHIVE_HOSTS
#             archive_top = ARCHIVE_DEFAULT_TOP
#         else:
#             archive_host = args[archive_host_str]
#             archive_repository = args[archive_respository_str]
#             archive_top = args[archive_top_str]
#
#         archive_status, archive_repositories, archive_message = photo_data.check_host_get_repositories(archive_host)
#
#         #     Create a cursor instance to selected host
#         #     if successful:
#         #         retreive repositories
#         #         retreive tops
#         #         return archive_host, archive_hosts, archive_repo, archive_repos, archive_top, archive_tops, archive_status, repo_count, top_count
#         #     else
#         #         host not available
#         return({archive_host_str: archive_host, archive_hosts_str: archive_hosts, archive_repositories_str: archive_repositories, archive_top_str: archive_top})



api.add_resource(TestJson, '/test_json')
api.add_resource(SetCollection, '/set_host')

if __name__ == '__main__':
    app.run(debug=True)