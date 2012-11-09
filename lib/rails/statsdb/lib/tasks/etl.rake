namespace :etl do
  task :set_utc do
    Time.zone = 'UTC'
  end
end
