module StatsdbPg
  class Base < ActiveRecord::Base
    establish_connection('statsdb_pg')
  end 
end
