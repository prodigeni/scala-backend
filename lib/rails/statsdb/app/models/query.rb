class Query < ActiveRecord::Base
  PATTERN = /\[([^:]+):([^:]+):([^]]+)\]/
  GROUPED_PATTERN = /(\[([^:]+):([^:]+):([^]]+)\])/

  has_many :query_executions, :dependent => :destroy

  serialize :parameters

  validates_presence_of :category
  validates_presence_of :name
  validates_presence_of :sql

  def short_desc
    (description || "").split("\n").first
  end

  def cache_path
    Rails.root.join('cache', 'queries', id.to_s)
  end

  def clear_cache
    FileUtils.rm_rf(cache_path)
  end 

public
  class << self
    def parameters(sql)
      sql ? sql.scan(::Query::PATTERN).uniq : []
    end
  end
end
