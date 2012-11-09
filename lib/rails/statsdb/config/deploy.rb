require "bundler/capistrano"

set :application, "statsdb"
set :deploy_to, "/opt/rails/statsdb"

set :repository,  "svn+ssh://svn.wikia-inc.com/svn/backend/lib/rails/statsdb"
set :scm, :subversion

role :web, "statsdb-s5"
role :app, "statsdb-s5"
role :db,  "statsdb-s5", :primary => true

set :use_sudo, false

set :normalize_asset_timestamps, false

after 'deploy:symlink', 'deploy:symlink_query_cache'

namespace :deploy do
  task :start do ; end
  task :stop do ; end
  task :restart, :roles => :app, :except => { :no_release => true } do
    run "#{try_sudo} touch #{File.join(current_path,'tmp','restart.txt')}"
  end

  task :symlink_query_cache do
    run "mkdir -p #{shared_path}/cache/queries"
    run "ln -nfs #{shared_path}/cache/queries #{current_path}/cache/queries"
  end
end

