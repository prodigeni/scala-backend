namespace :etl do
  namespace :database do

    desc "Test ActiveRecord-based connection using mysql gem"
    task :test => [:environment, :set_utc] do
      db = Statsdb::Base.connection
      result = db.execute('SELECT now() AS ts')
      puts result.fields.join(',')
      result.each { |row| puts result.fields.map { |field| row[field] }.join(',') }
    end 

  end
end
