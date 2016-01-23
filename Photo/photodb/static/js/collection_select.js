/**
 * Created by scott_jackson on 5/31/2015.
 */
photoApp = angular.module('ui_select_collection', ['ui.bootstrap']);
photoApp.controller('CollectionSelect', function ($scope, $log, $http) {
/*
    $http.get('path/path.json').success(function(data){
        $scope.collection = data;
    });
*/
    $scope.collection = {host: 'localhost'};
    $scope.hosts = [
      'localhost',
      'barney',
      'smithers',
      'google',
      'other'
    ];
    $scope.connect_status = "Disconnected";

    $scope.collection.repo = 'barney';
    $scope.repos = [
        'smithers',
        'barney',
        'test'
    ];

    $scope.select_host = function(){
        $http.post('/set_host', '{"host":' + $scope.host + '}').success(function(data){
            $scope.host = data.host;
            $scope.repos = data.repos;
        });
    };

  $scope.status = {
    isopen: false
  };

  $scope.toggled = function(open) {
    $log.log('Dropdown is now: ', open);
  };

  $scope.toggleDropdown = function($event) {
    $event.preventDefault();
    $event.stopPropagation();
    $scope.status.isopen = !$scope.status.isopen;
  };
});