class QueriesController < ApplicationController

  # GET /queries
  # GET /queries.json
  def index
    @categories = Query.select('DISTINCT category').order('category')
    @title = "Queries"

    queries = Query.order(:category, :name, :updated_at)

    if params[:category] and params[:category] != 'all'
      queries = queries.where(:category => params[:category])
      @category = params[:category]
    end 
    
    query_dedup = {}
    queries.each do |q|
      query_dedup["#{q.category}.#{q.name}"] = q
    end

    @queries = []
    query_dedup.keys.sort.each do |k|
      @queries << query_dedup[k]
    end

    respond_to do |format|
      format.html # index.html.erb
      format.json { render :json => @queries }
    end
  end

  # GET /queries/1
  # GET /queries/1.json
  def show
    @query = Query.find(params[:id])
    @query_versions = Query.where(:category => @query.category, :name => @query.name).order('updated_at DESC')

    @parameters = []
    @executions = []
        
    @query_versions.each do |version| 
      @parameters[version.id] = version.parameters
      @executions[version.id] = QueryExecution.where(:query_id => version.id, :result => 'Succeeded').order('parameters, started_at')

      executions_dedup = {}
      @executions[version.id].each do |e|
        executions_dedup["#{e.parameters}"] = e
      end

      @executions[version.id] = []
      executions_dedup.keys.sort.each do |k|
        @executions[version.id] << executions_dedup[k]
      end
    end 
        
    @title = @query.name

    respond_to do |format|
      format.html # show.html.erb
      format.json { render :json => @query }
    end
  end

  # GET /queries/new
  # GET /queries/new.json
  def new
    @query = Query.new

    respond_to do |format|
      format.html # new.html.erb
      format.json { render :json => @query }
    end
  end

  # GET /queries/1/edit
  def edit
    @query = Query.find(params[:id])
  end

  # POST /queries
  # POST /queries.json
  def create
    params[:query][:parameters] = Query.parameters(params[:query][:sql])

# TODO: check for existing query with same category, name and parameters

    @query = Query.new(params[:query])

    respond_to do |format|
      if @query.save
        format.html { redirect_to @query, :notice => 'Query was successfully created.' }
        format.json { render :json => @query, :status => :created, :location => @query }
      else
        format.html { render :action => "new" }
        format.json { render :json => @query.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /queries/1
  # PUT /queries/1.json
  def update
    params[:query][:parameters] = Query.parameters(params[:query][:sql])

    @query = Query.find(params[:id])

    if params[:query][:parameters].sort.map { |p| p[0] } != @query.parameters.sort.map { |p| p[0] }
      @query = Query.where(:category => params[:query][:category], :name => params[:query][:name], :parameters => params[:query][:parameters].to_yaml ).order('id desc').first

      if @query.nil?
        @query = Query.new(params[:query])
      end
    end

    respond_to do |format|
      if @query.update_attributes(params[:query])
        format.html { redirect_to @query, :notice => 'Query was successfully updated.' }
        format.json { head :ok }
      else
        format.html { render :action => "edit" }
        format.json { render :json => @query.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /queries/1
  # DELETE /queries/1.json
  def destroy
    @query = Query.find(params[:id])
    @query.destroy

    respond_to do |format|
      format.html { redirect_to queries_url }
      format.json { head :ok }
    end
  end
end
