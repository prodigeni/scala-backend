require 'test_helper'

class QueryExecutionsControllerTest < ActionController::TestCase
  setup do
    @query_execution = query_executions(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:query_executions)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create query_execution" do
    assert_difference('QueryExecution.count') do
      post :create, :query_execution => @query_execution.attributes
    end

    assert_redirected_to query_execution_path(assigns(:query_execution))
  end

  test "should show query_execution" do
    get :show, :id => @query_execution.to_param
    assert_response :success
  end

  test "should get edit" do
    get :edit, :id => @query_execution.to_param
    assert_response :success
  end

  test "should update query_execution" do
    put :update, :id => @query_execution.to_param, :query_execution => @query_execution.attributes
    assert_redirected_to query_execution_path(assigns(:query_execution))
  end

  test "should destroy query_execution" do
    assert_difference('QueryExecution.count', -1) do
      delete :destroy, :id => @query_execution.to_param
    end

    assert_redirected_to query_executions_path
  end
end
