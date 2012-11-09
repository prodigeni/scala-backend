#!/usr/bin/ruby

require 'rubygems'
require 'garb'
require 'active_support/all'
require 'csv'
require 'sequel'

@ga_user = 'wikiastats@gmail.com'
@ga_pass = 'wyodHuv2'
@session = Garb::Session.login(@ga_user, @ga_pass, :proxy => 'http://squid-proxy.local:3128/')
# MD analytics account
@profile = Garb::Profile.all[0]
@today = Time.now.midnight.utc
@start_date = (@today - 2.year)

GA_DB = Sequel.mysql('google_analytics', :user => 'google_analytics',
                  :host => 'statsdb-s3', :password => 'LaddArd5')
WIKI_DB = Sequel.mysql('wikicities', :user => 'wikia_mw',
                  :host => '10.8.32.26', :password => 'ahShu8eb')

unless GA_DB.table_exists?(:pageviews)
  GA_DB.create_table :pageviews do
    primary_key :id
    integer :city_id
    date :date
    integer :pageviews
    integer :entrances
    integer :exits
    integer :bounces
  end
  GA_DB.add_index :pageviews, [:city_id, :date]
end

unless GA_DB.table_exists?(:pageview_summary)
  GA_DB.create_table :pageview_summary do
    primary_key :id
    date :date
    DateTime :last_update
  end
end

class Pageviews < Sequel::Model
  set_dataset GA_DB[:pageviews]
end

class PageviewSummary < Sequel::Model
  set_dataset GA_DB[:pageview_summary]
end

class CatMap < Sequel::Model
  set_primary_key [ :city_id, :cat_id ]
  set_dataset WIKI_DB[:city_cat_mapping].select(:city_id, :cat_id)
  many_to_one :category, :key => :cat_id
  many_to_one :city, :key => :city_id
end

class City < Sequel::Model
  set_primary_key :city_id
  set_dataset WIKI_DB[:city_list].select(:city_id, :city_dbname, :city_lang, :city_url)
  one_to_many :domains
  many_to_many :categories, :join_table => :city_cat_mapping,
  :right_key => :city_id, :left_key => :cat_id
end

class Domain < Sequel::Model
  set_primary_key :city_id
  set_dataset WIKI_DB[:city_domains].select(:city_id, :city_domain)
  many_to_one :city
end

class Category < Sequel::Model
  set_primary_key :cat_id
  set_dataset WIKI_DB[:city_cats].select(:cat_id, :cat_name)
  many_to_many :cities, :join_table => :city_cat_mapping, 
  :right_key => :cat_id, :left_key => :city_id
end

class Tag < Sequel::Model
  set_primary_key :id
  set_dataset WIKI_DB[:city_tag].select(:id, :name)
  many_to_many :cities, :join_table => :city_tag_map, 
  :right_key => :cat_id, :left_key => :tag_id
end

def needs_updated?(date)
  puts "Checking for last update ..."
  summary = PageviewSummary.filter(:date => date.to_date).first
  if summary && (summary.date + 7.day) < Time.now.to_date
    puts "Data current:  #{date.month}/#{date.day}/#{date.year}"
    return false
  else
    Pageviews.filter(:date => date.to_date).delete
    return true
  end
end    

def combine(results)
  puts "Merging results"
  merged = {}
  results.each do |dom,metrics|
    dom_city = Domain.filter(:city_domain => dom).first
    if dom_city
      city_id = dom_city.city_id
      if merged[city_id]
        merged[city_id][:pageviews] = merged[city_id][:pageviews] + metrics[:pageviews]
        merged[city_id][:bounces] = merged[city_id][:bounces] + metrics[:bounces]
        merged[city_id][:entrances] = merged[city_id][:entrances] + metrics[:entrances]
        merged[city_id][:exits] = merged[city_id][:exits] + metrics[:exits]
      else
        merged[city_id] = Hash.new
        merged[city_id][:pageviews] = metrics[:pageviews]
        merged[city_id][:bounces] = metrics[:bounces]
        merged[city_id][:entrances] = metrics[:entrances]
        merged[city_id][:exits] = metrics[:exits]
      end
    end
  end
  return merged
end

def update_db(date,results)
  GA_DB.transaction do
    puts "Inserting: #{date.month}/#{date.day}/#{date.year}"
    results.each do |city_id,metrics|
      Pageviews.insert(:city_id => city_id,
                       :date => date,
                       :pageviews => metrics[:pageviews],
                       :bounces => metrics[:bounces],
                       :entrances => metrics[:entrances],
                       :exits => metrics[:exits])
                     end
    PageviewSummary.insert(:date => date,
                           :last_update => Time.now)
  end
end

def retrieve_date(date)
  offset = 1
  results = {}
  puts "Retrieving: #{date.month}/#{date.day}/#{date.year}"
  begin
    retries = 5
    interim_results = {}
    begin
      report = Garb::Report.new(@profile, :limit => 1000,
                                :offset => offset,
                                :start_date => date.beginning_of_day,
                                :end_date => date.end_of_day)
      report.metrics :pageviews, :bounces, :entrances, :exits
      report.dimensions :pagePath
      report.results.each do |row|
        interim_results[row.page_path.chop] = { 
          :pageviews => row.pageviews.to_i,
          :bounces => row.bounces.to_i,
          :entrances => row.entrances.to_i,
          :exits => row.exits.to_i
        }
      end
    rescue EOFError, Timeout::Error
      if retries > 0
        puts "Google timed out, retrying"
        sleep(30)
        interim_results = {}
        retries -= 1
        retry
      else
        raise
      end
    end
    results.merge!(interim_results)
    offset += 1000
    puts " Got #{results.size} records"
  end while report.results.length == 1000
  return results
end

begin
  if needs_updated?(@start_date)
    results = retrieve_date(@start_date)
    merged = combine(results)
    update_db(@start_date,merged)
  end
  @start_date = @start_date + 1.day
end while @start_date < @today
