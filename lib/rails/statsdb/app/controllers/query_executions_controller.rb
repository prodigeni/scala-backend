class QueryExecutionsController < ApplicationController
  # GET /query_executions
  # GET /query_executions.json
  def index
    @query_executions = QueryExecution.where(:query_id => params[:query_id])

    respond_to do |format|
      format.html # index.html.erb
      format.json { render :json => @query_executions }
    end
  end

  # GET /query_executions/1
  # GET /query_executions/1.json
  def show
    @query_execution = QueryExecution.find(params[:id])
    @query = Query.find(@query_execution.query_id)

    begin
      @data = @query_execution.execute(:overwrite => params[:refresh] || false)
    rescue StandardError => err 
      @error = err 
    end 
    
    @title = "#{@query.name} Results"
    
    respond_to do |format|
      format.html # show.html.erb
      format.json { render :json => @data }
      format.csv do
        headers["Content-Disposition"] = "attachment;filename=\"#{@query_execution.to_filename}\""
      end 
    end
  end

  # GET /query_executions/new
  # GET /query_executions/new.json
  def new
    @query_execution = QueryExecution.new

    respond_to do |format|
      format.html # new.html.erb
      format.json { render :json => @query_execution }
    end
  end

  # GET /query_executions/1/edit
  def edit
    @query_execution = QueryExecution.find(params[:id])
  end

  # POST /query_executions
  # POST /query_executions.json
  def create
    @query = Query.find(params[:query_id])

    @query_execution = QueryExecution.create( :query_id   => @query.id,
                                              :sql        => @query.sql,
                                              :parameters => params[:query] )

    redirect_to query_execution_url(@query, @query_execution)
  end

  # PUT /query_executions/1
  # PUT /query_executions/1.json
  def update
    @query_execution = QueryExecution.find(params[:id])

    respond_to do |format|
      if @query_execution.update_attributes(params[:query_execution])
        format.html { redirect_to @query_execution, :notice => 'Query execution was successfully updated.' }
        format.json { head :ok }
      else
        format.html { render :action => "edit" }
        format.json { render :json => @query_execution.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /query_executions/1
  # DELETE /query_executions/1.json
  def destroy
    @query_execution = QueryExecution.find(params[:id])
    @query_execution.destroy

    respond_to do |format|
      format.html { redirect_to query_executions_url }
      format.json { head :ok }
    end
  end
end
