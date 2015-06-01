/**
 * Created by scott_jackson on 5/31/2015.
 */
angular.module('ui.bootstrap.demo', ['ui.bootstrap']);
angular.module('ui.bootstrap.demo').controller('CollectionSelect', function ($scope, $log) {
    $scope.selected_host = {host: 'localhost'};
    $scope.hosts = [
      'localhost',
      'barney',
      'smithers',
      'google',
      'other'
    ];
/*    $scope.selected_host = {repo: 'barney'};
    $scope.repos = [
        'smithers',
        'barney',
        'test'
    ];*/
  $scope.connect_status = "Disconnected";

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